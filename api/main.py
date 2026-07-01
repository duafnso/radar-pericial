"""
api/main.py — Radar Pericial v2
FastAPI: serve o HTML, expõe REST API com dados reais do banco PostGIS
e carrega dados demo automaticamente no startup (se configurado).
"""

import logging
import os
import sys
import signal
import asyncio
import subprocess
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Optional

# ── Logger definido ANTES de qualquer uso ──────────────────────────────────
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s',
    stream=sys.stdout
)

# ── Signal handler (apenas log, não interfere no shutdown) ─────────────────
def _signal_handler(signum, frame):
    """Handler para sinais - apenas loga, não impede shutdown do Railway"""
    logger.info(f"🚨 [SIGNAL {signum}] Health check ou shutdown solicitado")

try:
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
except Exception as e:
    logger.warning(f"⚠️ Não foi possível registrar signal handlers: {e}")

# ── Imports do FastAPI e dependências ─────────────────────────────────────
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── Configuração de caminhos ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

# ── Imports do projeto ────────────────────────────────────────────────────
from database.db import Database, init_db
from intelligence.taxonomy import calcular_score, TAXONOMIA, REGIOES_IMEA

# ── Instância global do banco ─────────────────────────────────────────────
_db: Optional[Database] = None
_LOGIN_FAILURES: dict[str, list[float]] = {}


# ── Carregamento Automático de Dados Demo ──────────────────────────────────
async def _run_demo_collection():
    """
    Executa coleta de dados demo automaticamente no startup.
    Controlado pela variável de ambiente LOAD_DEMO_DATA=true
    """
    if os.getenv("LOAD_DEMO_DATA", "").lower() != "true":
        logger.info("ℹ️  LOAD_DEMO_DATA desativado - pulando dados demo")
        return
    
    logger.info("🔄 LOAD_DEMO_DATA ativado - iniciando coleta demo automática...")
    
    try:
        # ✅ Aponta para o arquivo correto que gera os dados demo solicitados
        demo_script = BASE_DIR / "working_data_collector.py"
        
        if not demo_script.exists():
            logger.warning(f"⚠️  working_data_collector.py não encontrado em: {demo_script}")
            return
        
        # Executa em thread separada para não bloquear o event loop do FastAPI
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(demo_script), "--source", "demo"],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos timeout
        )
        
        if result.returncode == 0:
            logger.info("✅ Dados demo carregados com sucesso!")
            # Loga as últimas linhas relevantes
            for line in result.stdout.strip().split('\n')[-10:]:
                if line.strip():
                    logger.info(f"   {line}")
        else:
            logger.error(f"❌ Erro na coleta demo: {result.stderr[-500:]}")
            
    except subprocess.TimeoutExpired:
        logger.error("⏱️  Timeout na coleta demo (300s)")
    except FileNotFoundError:
        logger.error("❌ Python ou script não encontrado no ambiente")
    except Exception as e:
        logger.error(f"❌ Exceção ao carregar dados demo: {type(e).__name__}: {e}")


# ── Lifespan: inicialização única do banco + demo ─────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa o banco UMA VEZ no startup + dados demo (opcional)."""
    global _db
    logger.info("🚀 [LIFESPAN] Iniciando aplicação...")
    
    try:
        # 1. Inicializa o banco de dados
        init_db()
        _db = Database()
        logger.info("✅ [LIFESPAN] Banco conectado e schema inicializado")
        
        # 2. Carrega dados demo automaticamente (se configurado)
        await _run_demo_collection()
        
        logger.info("✅ [LIFESPAN] API pronta para requests!")
        
    except Exception as e:
        logger.error(f"❌ [LIFESPAN] Falha crítica no startup: {e}")
        raise  # Re-raise para o Railway detectar falha
    
    yield  # ← ESSENCIAL: mantém o app rodando
    logger.info("🛑 [LIFESPAN] Encerrando aplicação...")


# ── Criação da aplicação FastAPI ──────────────────────────────────────────
def _is_production() -> bool:
    return os.getenv("APP_ENV", os.getenv("ENV", "development")).strip().lower() in {
        "prod",
        "production",
    }


def _env_enabled(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_cors_origins() -> list[str]:
    raw = os.getenv("CORS_ALLOW_ORIGINS")
    if raw is None:
        if _is_production():
            raise RuntimeError("CORS_ALLOW_ORIGINS deve ser definido em produção.")
        return ["http://localhost:8000", "http://127.0.0.1:8000"]
    origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
    if _is_production() and "*" in origins:
        raise RuntimeError("CORS_ALLOW_ORIGINS não pode conter '*' em produção.")
    return origins


api_docs_enabled = (not _is_production()) or _env_enabled("ENABLE_API_DOCS", False)


def _login_limits() -> tuple[int, int]:
    max_attempts = int(os.getenv("LOGIN_MAX_ATTEMPTS", "5"))
    window_seconds = int(os.getenv("LOGIN_WINDOW_SECONDS", "300"))
    return max_attempts, window_seconds


def _login_key(request: Request, username: str) -> str:
    client_ip = request.client.host if request.client else "unknown"
    return f"{client_ip}:{username.strip().lower()}"


def _prune_login_failures(key: str, now: float, window_seconds: int) -> list[float]:
    failures = [
        ts for ts in _LOGIN_FAILURES.get(key, [])
        if now - ts <= window_seconds
    ]
    if failures:
        _LOGIN_FAILURES[key] = failures
    else:
        _LOGIN_FAILURES.pop(key, None)
    return failures


def _assert_login_allowed(request: Request, username: str) -> str:
    max_attempts, window_seconds = _login_limits()
    key = _login_key(request, username)
    failures = _prune_login_failures(key, time.monotonic(), window_seconds)
    if len(failures) >= max_attempts:
        raise HTTPException(
            status_code=429,
            detail="Muitas tentativas de login. Tente novamente mais tarde.",
        )
    return key


def _record_login_failure(key: str) -> None:
    _LOGIN_FAILURES.setdefault(key, []).append(time.monotonic())


def _clear_login_failures(key: str) -> None:
    _LOGIN_FAILURES.pop(key, None)


app = FastAPI(
    title="Radar Pericial",
    version="2.0",
    docs_url="/docs" if api_docs_enabled else None,
    redoc_url="/redoc" if api_docs_enabled else None,
    openapi_url="/openapi.json" if api_docs_enabled else None,
    lifespan=lifespan
)

# CORS middleware
cors_origins = _parse_cors_origins()
allow_credentials = len(cors_origins) > 0 and "*" not in cors_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health Checks para Railway (SEM autenticação, respondem em <100ms) ───
@app.middleware("http")
async def security_headers(request, call_next):
    response = await call_next(request)
    headers = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "strict-origin-when-cross-origin",
        "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
    }
    for name, value in headers.items():
        if name not in response.headers:
            response.headers[name] = value
    return response


@app.get("/health")
async def railway_health():
    """Health check mínimo para Railway — SEM query no banco"""
    return {"status": "healthy", "service": "radar-pericial"}

@app.get("/health/live")
async def liveness():
    """Liveness probe: verifica se a aplicação está viva."""
    return {"status": "alive", "service": "radar-pericial"}

@app.get("/health/ready")
async def readiness():
    """Readiness probe: verifica dependências essenciais."""
    deps = {"database": False, "redis": False, "celery": False}
    details = {}

    try:
        if not _db:
            raise RuntimeError("Database não inicializado")
        _db.query("SELECT 1")
        deps["database"] = True
    except Exception as e:
        details["database_error"] = str(e)

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    rc = None
    try:
        import redis
        rc = redis.from_url(redis_url, socket_connect_timeout=2, socket_timeout=2)
        deps["redis"] = bool(rc.ping())
    except Exception as e:
        details["redis_error"] = str(e)
    finally:
        if rc is not None:
            rc.close()

    try:
        from alerts.scheduler import app as celery_app
        with celery_app.connection_for_read() as conn:
            conn.ensure_connection(max_retries=1)
        deps["celery"] = True
    except Exception as e:
        details["celery_error"] = str(e)

    all_ready = all(deps.values())
    payload = {"status": "ready" if all_ready else "degraded", "dependencies": deps, "details": details}
    if not all_ready:
        return JSONResponse(status_code=503, content=payload)
    return payload

@app.get("/")
async def root():
    """Rota raiz: serve HTML se existir, senão retorna JSON"""
    possible_paths = [
        BASE_DIR / "interface" / "templates" / "index.html",
        Path("/app/interface/templates/index.html"),
        Path(__file__).parent.parent.parent / "interface" / "templates" / "index.html",
    ]
    
    for html_path in possible_paths:
        if html_path.exists():
            try:
                html_content = html_path.read_text(encoding="utf-8")
                logger.info(f"✅ index.html servido de: {html_path}")
                return HTMLResponse(content=html_content, media_type="text/html")
            except Exception as e:
                logger.error(f"❌ Erro ao ler index.html em {html_path}: {e}")
                break
    
    logger.warning("⚠️ index.html não encontrado - retornando JSON")
    return JSONResponse({
        "message": "🚀 Radar Pericial API está rodando!",
        "docs": "/docs",
        "health": "/health",
        "status": "API ativa"
    })

@app.get("/index.html", response_class=HTMLResponse)
async def index_html():
    """Redireciona index.html para a rota raiz"""
    return await root()


# ── Middleware de log de requests (opcional, útil para debug) ─────────────
@app.middleware("http")
async def log_requests(request, call_next):
    path = request.url.path
    if path not in ["/health", "/", "/docs", "/openapi.json", "/redoc"]:
        logger.info(f"📥 {request.method} {path}")
    response = await call_next(request)
    if path not in ["/health", "/", "/docs", "/openapi.json", "/redoc"]:
        logger.info(f"📤 {request.method} {path} → {response.status_code}")
    return response


# ── Static files ──────────────────────────────────────────────────────────
static_dir = BASE_DIR / "interface" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── Dependência de autenticação ───────────────────────────────────────────
def get_current_user(
    request: Request,
    authorization: Annotated[Optional[str], Header()] = None,
) -> dict:
    """Valida token Bearer. Lança 401 se inválido."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    token = authorization.split(" ", 1)[1].strip()
    if not _db:
        raise HTTPException(status_code=503, detail="Banco de dados não inicializado")
    client_ip = request.client.host if request.client else None
    user = _db.validate_token_user(
        token,
        user_agent=request.headers.get("user-agent"),
        client_ip=client_ip,
    )
    if not user:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")
    return user

AuthUser = Annotated[dict, Depends(get_current_user)]


def get_current_admin(user: AuthUser) -> dict:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Acesso restrito ao administrador")
    return user


AdminUser = Annotated[dict, Depends(get_current_admin)]


def _audit_request(
    request: Request,
    acao: str,
    ator: Optional[dict] = None,
    entidade: Optional[str] = None,
    entidade_id: Optional[str] = None,
    detalhes: Optional[dict] = None,
) -> None:
    if not _db:
        return
    client_ip = request.client.host if request.client else None
    _db.registrar_auditoria(
        acao=acao,
        ator=ator,
        entidade=entidade,
        entidade_id=entidade_id,
        detalhes=detalhes,
        ip=client_ip,
        user_agent=request.headers.get("user-agent"),
    )


# ── Modelos Pydantic ──────────────────────────────────────────────────────
class LoginInput(BaseModel):
    username: str
    password: str

class ScoreInput(BaseModel):
    classe_processual: str = ""
    assunto: str = ""
    movimentacoes: list[str] = []
    eventos_admin: list[str] = []
    texto_livre: str = ""

class PeritoInput(BaseModel):
    nome: str
    registro_profissional: str = ""
    especialidades: str = ""
    municipios_atuacao: str = ""
    regiao_imea: str = ""

class UsuarioInput(BaseModel):
    username: str
    password: str
    role: str = "user"
    regiao_foco: Optional[str] = None

class UsuarioRoleInput(BaseModel):
    role: str

class UsuarioAtivoInput(BaseModel):
    ativo: bool

class UsuarioSenhaInput(BaseModel):
    password: str


# ── Autenticação ─────────────────────────────────────────────────────────
@app.post("/api/login")
async def login(body: LoginInput, request: Request):
    username = body.username.strip().lower()
    login_key = _assert_login_allowed(request, username)
    if not _db or not _db.check_login(username, body.password):
        _record_login_failure(login_key)
        _audit_request(
            request,
            "login_failed",
            ator={"username": username},
            entidade="usuario",
            entidade_id=username,
        )
        raise HTTPException(status_code=401, detail="Credenciais incorretas")
    client_ip = request.client.host if request.client else None
    token = _db.create_token(
        username,
        user_agent=request.headers.get("user-agent"),
        client_ip=client_ip,
    )
    _clear_login_failures(login_key)
    _audit_request(
        request,
        "login_success",
        ator={"username": username},
        entidade="usuario",
        entidade_id=username,
    )
    return {"status": "ok", "token": token}

@app.post("/api/logout")
async def logout(
    request: Request,
    _user: AuthUser,
    authorization: Annotated[Optional[str], Header()] = None,
):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    if not _db:
        raise HTTPException(status_code=503, detail="Banco de dados não inicializado")
    token = authorization.split(" ", 1)[1].strip()
    _db.revoke_token(token)
    _audit_request(request, "logout", ator=_user, entidade="usuario", entidade_id=_user.get("id"))
    return {"status": "ok"}

@app.get("/api/health")
async def api_health(_user: AuthUser):
    """Health check da API — requer autenticação"""
    return {
        "status": "ok",
        "service": "Radar Pericial v2",
        "authenticated": True,
        "id": _user.get("id"),
        "user": _user.get("username"),
        "role": _user.get("role"),
    }


# ── Stats ────────────────────────────────────────────────────────────────
@app.get("/api/stats")
async def stats(regiao: Optional[str] = Query(None), _user: AuthUser = None):
    try:
        if not _db: return {}
        s = _db.stats(regiao)
        return {k: (int(v) if isinstance(v, (int, float)) else str(v or "")) for k, v in s.items()}
    except Exception as e:
        logger.error(f"stats: {e}")
        return {}


# ── Processos ────────────────────────────────────────────────────────────
@app.get("/api/processos")
async def processos(
    faixa: Optional[str] = Query(None), municipio: Optional[str] = Query(None),
    regiao: Optional[str] = Query(None), classe: Optional[str] = Query(None),
    limit: int = Query(20, le=500), offset: int = Query(0), _user: AuthUser = None
):
    try:
        if not _db: return {"total": 0, "items": []}
        w, p = [], {"limit": limit, "offset": offset}
        if faixa: w.append("s.faixa_probabilidade = :faixa"); p["faixa"] = faixa
        if municipio: w.append("p.municipio ILIKE :mun"); p["mun"] = f"%{municipio}%"
        if regiao: w.append("p.regiao_imea = :regiao"); p["regiao"] = regiao
        if classe: w.append("p.classe_processual ILIKE :classe"); p["classe"] = f"%{classe}%"
        where = ("WHERE " + " AND ".join(w)) if w else ""
        sql = f"""
            SELECT p.id, p.numero_cnj, p.tribunal, p.comarca, p.vara,
                   p.classe_processual, p.assunto_principal,
                   p.data_distribuicao::text AS data_distribuicao,
                   p.fase_atual, p.municipio, p.regiao_imea, p.origem,
                   s.score_total, s.faixa_probabilidade, s.faixa_label,
                   s.tipo_pericia_sugerida, s.categorias_detectadas, s.urgencia
            FROM processos p LEFT JOIN score_pericial s ON s.processo_id = p.id
            {where} ORDER BY s.score_total DESC NULLS LAST LIMIT :limit OFFSET :offset
        """
        df = _db.query(sql, p)
        cnt_p = {k: v for k, v in p.items() if k not in ("limit", "offset")}
        total = int(_db.query(f"SELECT COUNT(*) FROM processos p LEFT JOIN score_pericial s ON s.processo_id=p.id {where}", cnt_p).iloc[0, 0])
        return {"total": total, "offset": offset, "limit": limit, "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"processos: {e}")
        return {"total": 0, "items": []}


# ── Eventos / Portarias ─────────────────────────────────────────────────
@app.get("/api/eventos")
async def eventos(
    municipio: Optional[str] = Query(None), faixa: Optional[str] = Query(None),
    fontes: Optional[str] = Query(None), dias: int = Query(30),
    limit: int = Query(50, le=300), offset: int = Query(0), _user: AuthUser = None
):
    try:
        if not _db: return {"total": 0, "items": []}
        w = ["coletado_em >= NOW() - INTERVAL '1 day' * :dias"]
        p = {"limit": limit, "offset": offset, "dias": dias}
        if municipio: w.append("municipio ILIKE :mun"); p["mun"] = f"%{municipio}%"
        if faixa: w.append("faixa_probabilidade = :faixa"); p["faixa"] = faixa
        if fontes:
            vals = [f.strip() for f in fontes.split(",") if f.strip()]
            if vals:
                marks = []
                for i, fv in enumerate(vals):
                    k = f"fonte_{i}"
                    p[k] = fv
                    marks.append(f":{k}")
                w.append(f"LOWER(fonte) IN ({','.join(f'LOWER({m})' for m in marks)})")
        where = "WHERE " + " AND ".join(w)
        sql = f"""
            SELECT id, titulo, resumo, data_publicacao::text AS data_publicacao,
                   municipio, area_ha, fonte, orgao, url,
                   categoria_agronomica, score_evento, faixa_probabilidade,
                   coletado_em::text AS coletado_em
            FROM portarias_diario_oficial {where}
            ORDER BY coletado_em DESC LIMIT :limit OFFSET :offset
        """
        df = _db.query(sql, p)
        cnt_p = {k: v for k, v in p.items() if k not in ("limit", "offset")}
        total = int(_db.query(f"SELECT COUNT(*) FROM portarias_diario_oficial {where}", cnt_p).iloc[0, 0])
        return {"total": total, "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"eventos: {e}")
        return {"total": 0, "items": []}


# ── GeoJSON Endpoints ───────────────────────────────────────────────────
@app.get("/api/parcelas/geojson")
async def parcelas_geojson(municipio: Optional[str] = Query(None), apenas_desapropriadas: bool = Query(False), _user: AuthUser = None):
    try:
        if not _db: return {"type": "FeatureCollection", "features": []}
        w = ["geometry IS NOT NULL"]
        p = {}
        if municipio: w.append("municipio ILIKE :mun"); p["mun"] = f"%{municipio}%"
        if apenas_desapropriadas: w.append("desapropriacao_flag = TRUE")
        sql = f"""
            SELECT codigo_imovel, municipio, area_ha, situacao, desapropriacao_flag, fonte,
                   ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom
            FROM parcelas_sigef WHERE {" AND ".join(w)} LIMIT 2000
        """
        df = _db.query(sql, p)
        feats = []
        for _, r in df.iterrows():
            if r.get("geom"):
                feats.append({"type": "Feature", "geometry": r["geom"], "properties": {
                    "codigo_imovel": r.get("codigo_imovel", ""), "municipio": r.get("municipio", ""),
                    "area_ha": float(r.get("area_ha") or 0), "situacao": r.get("situacao", ""),
                    "desapropriacao_flag": bool(r.get("desapropriacao_flag")), "fonte": r.get("fonte", "SIGEF")
                }})
        return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        logger.error(f"parcelas geojson: {e}")
        return {"type": "FeatureCollection", "features": []}

@app.get("/api/municipios/geojson")
async def municipios_geojson(_user: AuthUser = None):
    try:
        if not _db: return {"type": "FeatureCollection", "features": []}
        df = _db.query("SELECT nome, regiao_imea, codigo_ibge, ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom FROM municipios_mt WHERE geometry IS NOT NULL")
        return {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": r["geom"], "properties": {"nome": r.get("nome",""), "regiao_imea": r.get("regiao_imea",""), "codigo_ibge": r.get("codigo_ibge","")}} for _, r in df.iterrows() if r.get("geom")]}
    except Exception as e:
        logger.error(f"municipios geojson: {e}")
        return {"type": "FeatureCollection", "features": []}

@app.get("/api/assentamentos/geojson")
async def assentamentos_geojson(_user: AuthUser = None):
    try:
        if not _db: return {"type": "FeatureCollection", "features": []}
        df = _db.query("SELECT nome_pa, municipio, area_ha, num_familias, fase, ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom FROM assentamentos_incra WHERE geometry IS NOT NULL LIMIT 500")
        return {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": r["geom"], "properties": {"nome_pa": r.get("nome_pa",""), "municipio": r.get("municipio",""), "area_ha": float(r.get("area_ha") or 0), "num_familias": r.get("num_familias","")}} for _, r in df.iterrows() if r.get("geom")]}
    except Exception as e:
        logger.error(f"assentamentos: {e}")
        return {"type": "FeatureCollection", "features": []}

@app.get("/api/prodes/geojson")
async def prodes_geojson(_user: AuthUser = None):
    try:
        if not _db: return {"type": "FeatureCollection", "features": []}
        df = _db.query("SELECT ano, area_km2, classe, ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom FROM inpe_prodes WHERE geometry IS NOT NULL LIMIT 500")
        return {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": r["geom"], "properties": {"ano": r.get("ano",""), "area_km2": float(r.get("area_km2") or 0), "classe": r.get("classe","")}} for _, r in df.iterrows() if r.get("geom")]}
    except Exception as e:
        logger.error(f"prodes: {e}")
        return {"type": "FeatureCollection", "features": []}


# ── Score Endpoints ─────────────────────────────────────────────────────
@app.post("/api/score/calcular")
async def score_calcular(body: ScoreInput, _user: AuthUser = None):
    return calcular_score(
        classe_processual=body.classe_processual, assunto=body.assunto,
        movimentacoes=body.movimentacoes, eventos_admin=body.eventos_admin,
        texto_livre=body.texto_livre
    ).to_dict()

@app.get("/api/score/distribuicao")
async def score_distribuicao(_user: AuthUser = None):
    try:
        if not _db: return {"frio": 0, "observacao": 0, "provavel": 0, "janela_quente": 0}
        df = _db.query("SELECT faixa_probabilidade, COUNT(*) AS total FROM score_pericial GROUP BY faixa_probabilidade")
        r = {"frio": 0, "observacao": 0, "provavel": 0, "janela_quente": 0}
        for _, row in df.iterrows():
            k = str(row["faixa_probabilidade"])
            if k in r: r[k] = int(row["total"])
        return r
    except Exception as e:
        logger.error(f"dist: {e}")
        return {"frio": 0, "observacao": 0, "provavel": 0, "janela_quente": 0}

@app.get("/api/score/regioes")
async def score_regioes(_user: AuthUser = None):
    try:
        if not _db: return []
        return _db.query("""
            SELECT p.regiao_imea, COUNT(*) AS total FROM processos p
            JOIN score_pericial s ON s.processo_id=p.id
            WHERE s.faixa_probabilidade IN ('janela_quente','provavel')
            GROUP BY p.regiao_imea ORDER BY total DESC
        """).fillna("").to_dict(orient="records")
    except Exception as e:
        logger.error(f"regioes: {e}")
        return []


# ── Peritos Endpoints ───────────────────────────────────────────────────
@app.get("/api/peritos")
async def peritos(regiao: Optional[str] = Query(None), busca: Optional[str] = Query(None), _user: AuthUser = None):
    try:
        if not _db: return {"total": 0, "items": []}
        w, p = [], {}
        if regiao: w.append("regiao_imea = :regiao"); p["regiao"] = regiao
        if busca: w.append("(nome ILIKE :b OR registro_profissional ILIKE :b)"); p["b"] = f"%{busca}%"
        where = ("WHERE " + " AND ".join(w)) if w else ""
        df = _db.query(f"SELECT * FROM peritos_agronomos {where} ORDER BY score_profissional DESC NULLS LAST", p)
        return {"total": len(df), "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"peritos: {e}")
        return {"total": 0, "items": []}

@app.post("/api/peritos")
async def criar_perito(body: PeritoInput, _user: AuthUser = None):
    try:
        if not _db: raise HTTPException(status_code=503, detail="Banco não inicializado")
        pid = _db.criar_perito(body.model_dump())
        return {"id": pid, "status": "created"}
    except Exception as e:
        logger.error(f"criar_perito: {e}")
        raise HTTPException(500, str(e))


# ── Alertas ─────────────────────────────────────────────────────────────
@app.get("/api/alertas")
async def alertas(limit: int = Query(40, le=200), _user: AuthUser = None):
    try:
        if not _db: return {"total": 0, "items": []}
        df = _db.query("""
            SELECT titulo, resumo, data_publicacao::text AS data_publicacao,
                   municipio, area_ha, fonte, orgao, url,
                   score_evento, faixa_probabilidade, coletado_em::text AS coletado_em
            FROM portarias_diario_oficial
            WHERE faixa_probabilidade IN ('janela_quente','provavel')
            ORDER BY coletado_em DESC LIMIT :limit
        """, {"limit": limit})
        return {"total": len(df), "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"alertas: {e}")
        return {"total": 0, "items": []}


@app.get("/api/coletas/status")
async def coletas_status(limit: int = Query(50, le=200), _user: AuthUser = None):
    try:
        if not _db:
            raise HTTPException(status_code=503, detail="Banco não inicializado")
        df = _db.listar_execucoes_coleta(limit=limit)
        return {"total": len(df), "items": df.fillna("").to_dict(orient="records")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"coletas_status: {e}")
        raise HTTPException(status_code=500, detail="Erro ao consultar status das coletas")


@app.post("/api/coletas/{tipo}/executar")
async def executar_coleta(tipo: str, request: Request, _admin: AdminUser):
    try:
        from alerts.scheduler import task_admin, task_geo, task_judicial, task_score

        tasks = {
            "geo": lambda: task_geo.delay(),
            "judicial": lambda: task_judicial.delay(dias_atras=1),
            "admin": lambda: task_admin.delay(dias_atras=2),
            "score": lambda: task_score.delay(),
        }
        if tipo not in tasks:
            raise HTTPException(status_code=400, detail="Tipo de coleta inválido")
        result = tasks[tipo]()
        _audit_request(
            request,
            "coleta_manual_enfileirada",
            ator=_admin,
            entidade="coleta",
            entidade_id=tipo,
            detalhes={"task_id": result.id},
        )
        return {"status": "queued", "tipo": tipo, "task_id": result.id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"executar_coleta {tipo}: {e}")
        raise HTTPException(status_code=500, detail="Erro ao enfileirar coleta")


def _validate_role(role: str) -> str:
    allowed = {"admin", "user", "viewer", "operator"}
    if role not in allowed:
        raise HTTPException(status_code=400, detail="Role invalida")
    return role


def _validate_password(password: str) -> None:
    if len(password or "") < 8:
        raise HTTPException(status_code=400, detail="Senha deve ter ao menos 8 caracteres")


@app.get("/api/admin/usuarios")
async def admin_listar_usuarios(_admin: AdminUser):
    try:
        if not _db:
            raise HTTPException(status_code=503, detail="Banco nao inicializado")
        df = _db.listar_usuarios()
        return {"total": len(df), "items": df.fillna("").to_dict(orient="records")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_listar_usuarios: {e}")
        raise HTTPException(status_code=500, detail="Erro ao listar usuarios")


@app.post("/api/admin/usuarios")
async def admin_criar_usuario(body: UsuarioInput, request: Request, _admin: AdminUser):
    try:
        if not _db:
            raise HTTPException(status_code=503, detail="Banco nao inicializado")
        username = body.username.strip().lower()
        if len(username) < 3:
            raise HTTPException(status_code=400, detail="Username deve ter ao menos 3 caracteres")
        _validate_password(body.password)
        role = _validate_role(body.role)
        user_id = _db.criar_usuario(
            username=username,
            password_raw=body.password,
            role=role,
            regiao_foco=body.regiao_foco,
        )
        _audit_request(
            request,
            "usuario_criado",
            ator=_admin,
            entidade="usuario",
            entidade_id=str(user_id),
            detalhes={"username": username, "role": role, "regiao_foco": body.regiao_foco},
        )
        return {"status": "created", "id": user_id}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"admin_criar_usuario: {e}")
        raise HTTPException(status_code=500, detail="Erro ao criar usuario")


@app.patch("/api/admin/usuarios/{user_id}/role")
async def admin_atualizar_role(user_id: int, body: UsuarioRoleInput, request: Request, _admin: AdminUser):
    try:
        if not _db:
            raise HTTPException(status_code=503, detail="Banco nao inicializado")
        role = _validate_role(body.role)
        if user_id == _admin.get("id") and role != "admin":
            raise HTTPException(status_code=400, detail="Admin nao pode remover o proprio papel")
        if not _db.atualizar_role_usuario(user_id, role):
            raise HTTPException(status_code=404, detail="Usuario nao encontrado")
        _audit_request(
            request,
            "usuario_role_atualizada",
            ator=_admin,
            entidade="usuario",
            entidade_id=str(user_id),
            detalhes={"role": role},
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_atualizar_role: {e}")
        raise HTTPException(status_code=500, detail="Erro ao atualizar role")


@app.patch("/api/admin/usuarios/{user_id}/ativo")
async def admin_definir_usuario_ativo(user_id: int, body: UsuarioAtivoInput, request: Request, _admin: AdminUser):
    try:
        if not _db:
            raise HTTPException(status_code=503, detail="Banco nao inicializado")
        if user_id == _admin.get("id") and not body.ativo:
            raise HTTPException(status_code=400, detail="Admin nao pode desativar a propria conta")
        if not _db.definir_usuario_ativo(user_id, body.ativo):
            raise HTTPException(status_code=404, detail="Usuario nao encontrado")
        _audit_request(
            request,
            "usuario_status_atualizado",
            ator=_admin,
            entidade="usuario",
            entidade_id=str(user_id),
            detalhes={"ativo": body.ativo},
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_definir_usuario_ativo: {e}")
        raise HTTPException(status_code=500, detail="Erro ao atualizar usuario")


@app.patch("/api/admin/usuarios/{user_id}/senha")
async def admin_redefinir_senha(user_id: int, body: UsuarioSenhaInput, request: Request, _admin: AdminUser):
    try:
        if not _db:
            raise HTTPException(status_code=503, detail="Banco nao inicializado")
        _validate_password(body.password)
        if not _db.redefinir_senha_usuario(user_id, body.password):
            raise HTTPException(status_code=404, detail="Usuario nao encontrado")
        _audit_request(
            request,
            "usuario_senha_redefinida",
            ator=_admin,
            entidade="usuario",
            entidade_id=str(user_id),
        )
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_redefinir_senha: {e}")
        raise HTTPException(status_code=500, detail="Erro ao redefinir senha")


@app.get("/api/admin/auditoria")
async def admin_listar_auditoria(_admin: AdminUser, limit: int = Query(100, le=500)):
    try:
        if not _db:
            raise HTTPException(status_code=503, detail="Banco nao inicializado")
        df = _db.listar_auditoria(limit=limit)
        return {"total": len(df), "items": df.fillna("").to_dict(orient="records")}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"admin_listar_auditoria: {e}")
        raise HTTPException(status_code=500, detail="Erro ao listar auditoria")
