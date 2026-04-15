"""
api/main.py — Radar Pericial v2
FastAPI: serve o HTML e expõe REST API com dados reais do banco PostGIS.

Fixes aplicados:
  - Database singleton via lifespan (init_db chamado uma única vez)
  - Instância _db global reutilizada em todos os endpoints
  - Autenticação real: tokens persistidos no banco, validados via Depends
  - Todos os endpoints sensíveis protegidos com get_current_user
"""

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from database.db import Database, init_db
from intelligence.taxonomy import calcular_score, TAXONOMIA, REGIOES_IMEA

logger = logging.getLogger(__name__)

# ── Instância global do banco ──────────────────────────────────────────────
_db: Database = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa o banco UMA VEZ no startup — nunca em cada requisição."""
    global _db
    init_db()
    _db = Database()
    logger.info("API iniciada, banco pronto.")
    yield
    logger.info("API encerrando.")


app = FastAPI(title="Radar Pericial", version="2.0", docs_url="/docs", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

static_dir = BASE_DIR / "interface" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── Dependência de autenticação ────────────────────────────────────────────
def get_current_user(
    authorization: Annotated[Optional[str], Header()] = None,
) -> str:
    """
    Valida o token Bearer enviado no header Authorization.
    Lança 401 se ausente, malformado ou expirado.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    token = authorization.split(" ", 1)[1].strip()
    username = _db.validate_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")
    return username


AuthUser = Annotated[str, Depends(get_current_user)]


# ── Autenticação ───────────────────────────────────────────────────────────
class LoginInput(BaseModel):
    username: str
    password: str


@app.post("/api/login")
async def login(body: LoginInput):
    if not _db.check_login(body.username, body.password):
        raise HTTPException(status_code=401, detail="Credenciais incorretas")
    token = _db.create_token(body.username)
    return {"status": "ok", "token": token}


# ── Frontend ───────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    f = BASE_DIR / "interface" / "templates" / "index.html"
    if not f.exists():
        raise HTTPException(404, "index.html não encontrado")
    return HTMLResponse(f.read_text(encoding="utf-8"))


@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "Radar Pericial v2"}


# ── Stats ──────────────────────────────────────────────────────────────────
@app.get("/api/stats")
async def stats(
    regiao: Optional[str] = Query(None),
    _user: AuthUser = None,
):
    try:
        s = _db.stats(regiao)
        return {k: (int(v) if isinstance(v, (int, float)) else str(v or ""))
                for k, v in s.items()}
    except Exception as e:
        logger.error(f"stats: {e}")
        return {}


# ── Processos ──────────────────────────────────────────────────────────────
@app.get("/api/processos")
async def processos(
    faixa:     Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    regiao:    Optional[str] = Query(None),
    classe:    Optional[str] = Query(None),
    limit:     int = Query(20, le=500),
    offset:    int = Query(0),
    _user:     AuthUser = None,
):
    try:
        w, p = [], {"limit": limit, "offset": offset}

        if faixa:     w.append("s.faixa_probabilidade = :faixa");    p["faixa"]  = faixa
        if municipio: w.append("p.municipio ILIKE :mun");            p["mun"]    = f"%{municipio}%"
        if regiao:    w.append("p.regiao_imea = :regiao");           p["regiao"] = regiao
        if classe:    w.append("p.classe_processual ILIKE :classe"); p["classe"] = f"%{classe}%"

        where = ("WHERE " + " AND ".join(w)) if w else ""
        sql = f"""
            SELECT p.id, p.numero_cnj, p.tribunal, p.comarca, p.vara,
                   p.classe_processual, p.assunto_principal,
                   p.data_distribuicao::text AS data_distribuicao,
                   p.fase_atual, p.municipio, p.regiao_imea, p.origem,
                   s.score_total, s.faixa_probabilidade, s.faixa_label,
                   s.tipo_pericia_sugerida, s.categorias_detectadas, s.urgencia
            FROM processos p
            LEFT JOIN score_pericial s ON s.processo_id = p.id
            {where}
            ORDER BY s.score_total DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """
        df = _db.query(sql, p)
        cnt_p = {k: v for k, v in p.items() if k not in ("limit", "offset")}
        total = int(
            _db.query(
                f"SELECT COUNT(*) FROM processos p "
                f"LEFT JOIN score_pericial s ON s.processo_id=p.id {where}",
                cnt_p,
            ).iloc[0, 0]
        )
        return {"total": total, "offset": offset, "limit": limit,
                "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"processos: {e}")
        return {"total": 0, "items": []}


# ── Eventos / Portarias ────────────────────────────────────────────────────
@app.get("/api/eventos")
async def eventos(
    municipio: Optional[str] = Query(None),
    faixa:     Optional[str] = Query(None),
    fontes:    Optional[str] = Query(None),
    dias:      int = Query(30),
    limit:     int = Query(50, le=300),
    offset:    int = Query(0),
    _user:     AuthUser = None,
):
    try:
        w = ["coletado_em >= NOW() - INTERVAL '1 day' * :dias"]
        p: dict = {"limit": limit, "offset": offset, "dias": dias}

        if municipio: w.append("municipio ILIKE :mun"); p["mun"] = f"%{municipio}%"
        if faixa:     w.append("faixa_probabilidade = :faixa"); p["faixa"] = faixa
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
            FROM portarias_diario_oficial
            {where}
            ORDER BY coletado_em DESC
            LIMIT :limit OFFSET :offset
        """
        df = _db.query(sql, p)
        cnt_p = {k: v for k, v in p.items() if k not in ("limit", "offset")}
        total = int(
            _db.query(
                f"SELECT COUNT(*) FROM portarias_diario_oficial {where}", cnt_p
            ).iloc[0, 0]
        )
        return {"total": total, "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"eventos: {e}")
        return {"total": 0, "items": []}


# ── GeoJSON Parcelas ───────────────────────────────────────────────────────
@app.get("/api/parcelas/geojson")
async def parcelas_geojson(
    municipio:             Optional[str] = Query(None),
    apenas_desapropriadas: bool = Query(False),
    _user:                 AuthUser = None,
):
    try:
        w = ["geometry IS NOT NULL"]
        p: dict = {}
        if municipio:             w.append("municipio ILIKE :mun"); p["mun"] = f"%{municipio}%"
        if apenas_desapropriadas: w.append("desapropriacao_flag = TRUE")

        sql = f"""
            SELECT codigo_imovel, municipio, area_ha, situacao,
                   desapropriacao_flag, fonte,
                   ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom
            FROM parcelas_sigef
            WHERE {" AND ".join(w)}
            LIMIT 2000
        """
        df = _db.query(sql, p)
        feats = []
        for _, r in df.iterrows():
            g = r.get("geom")
            if g is None:
                continue
            feats.append({
                "type": "Feature", "geometry": g,
                "properties": {
                    "codigo_imovel":       r.get("codigo_imovel", ""),
                    "municipio":           r.get("municipio", ""),
                    "area_ha":             float(r.get("area_ha") or 0),
                    "situacao":            r.get("situacao", ""),
                    "desapropriacao_flag": bool(r.get("desapropriacao_flag")),
                    "fonte":               r.get("fonte", "SIGEF"),
                },
            })
        return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        logger.error(f"parcelas geojson: {e}")
        return {"type": "FeatureCollection", "features": []}


# ── GeoJSON Municípios ─────────────────────────────────────────────────────
@app.get("/api/municipios/geojson")
async def municipios_geojson(_user: AuthUser = None):
    try:
        sql = """
            SELECT nome, regiao_imea, codigo_ibge,
                   ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom
            FROM municipios_mt WHERE geometry IS NOT NULL
        """
        df = _db.query(sql)
        feats = []
        for _, r in df.iterrows():
            g = r.get("geom")
            if g is None:
                continue
            feats.append({
                "type": "Feature", "geometry": g,
                "properties": {
                    "nome":        r.get("nome", ""),
                    "regiao_imea": r.get("regiao_imea", ""),
                    "codigo_ibge": r.get("codigo_ibge", ""),
                },
            })
        return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        logger.error(f"municipios geojson: {e}")
        return {"type": "FeatureCollection", "features": []}


# ── GeoJSON Assentamentos ──────────────────────────────────────────────────
@app.get("/api/assentamentos/geojson")
async def assentamentos_geojson(_user: AuthUser = None):
    try:
        sql = """
            SELECT nome_pa, municipio, area_ha, num_familias, fase,
                   ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom
            FROM assentamentos_incra WHERE geometry IS NOT NULL LIMIT 500
        """
        df = _db.query(sql)
        feats = []
        for _, r in df.iterrows():
            g = r.get("geom")
            if g is None:
                continue
            feats.append({
                "type": "Feature", "geometry": g,
                "properties": {
                    "nome_pa":      r.get("nome_pa", ""),
                    "municipio":    r.get("municipio", ""),
                    "area_ha":      float(r.get("area_ha") or 0),
                    "num_familias": r.get("num_familias", ""),
                },
            })
        return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        logger.error(f"assentamentos: {e}")
        return {"type": "FeatureCollection", "features": []}


# ── GeoJSON PRODES ─────────────────────────────────────────────────────────
@app.get("/api/prodes/geojson")
async def prodes_geojson(_user: AuthUser = None):
    try:
        sql = """
            SELECT ano, area_km2, classe,
                   ST_AsGeoJSON(ST_ForcePolygonCCW(geometry))::json AS geom
            FROM inpe_prodes WHERE geometry IS NOT NULL LIMIT 500
        """
        df = _db.query(sql)
        feats = []
        for _, r in df.iterrows():
            g = r.get("geom")
            if g is None:
                continue
            feats.append({
                "type": "Feature", "geometry": g,
                "properties": {
                    "ano":     r.get("ano", ""),
                    "area_km2": float(r.get("area_km2") or 0),
                    "classe":  r.get("classe", ""),
                },
            })
        return {"type": "FeatureCollection", "features": feats}
    except Exception as e:
        logger.error(f"prodes: {e}")
        return {"type": "FeatureCollection", "features": []}


# ── Score ──────────────────────────────────────────────────────────────────
class ScoreInput(BaseModel):
    classe_processual: str = ""
    assunto:           str = ""
    movimentacoes:     list[str] = []
    eventos_admin:     list[str] = []
    texto_livre:       str = ""


@app.post("/api/score/calcular")
async def score_calcular(body: ScoreInput, _user: AuthUser = None):
    r = calcular_score(
        classe_processual=body.classe_processual,
        assunto=body.assunto,
        movimentacoes=body.movimentacoes,
        eventos_admin=body.eventos_admin,
        texto_livre=body.texto_livre,
    )
    return r.to_dict()


@app.get("/api/score/distribuicao")
async def score_distribuicao(_user: AuthUser = None):
    try:
        df = _db.query(
            "SELECT faixa_probabilidade, COUNT(*) AS total "
            "FROM score_pericial GROUP BY faixa_probabilidade"
        )
        r = {"frio": 0, "observacao": 0, "provavel": 0, "janela_quente": 0}
        for _, row in df.iterrows():
            k = str(row["faixa_probabilidade"])
            if k in r:
                r[k] = int(row["total"])
        return r
    except Exception as e:
        logger.error(f"dist: {e}")
        return {"frio": 0, "observacao": 0, "provavel": 0, "janela_quente": 0}


@app.get("/api/score/regioes")
async def score_regioes(_user: AuthUser = None):
    try:
        df = _db.query("""
            SELECT p.regiao_imea, COUNT(*) AS total
            FROM processos p JOIN score_pericial s ON s.processo_id=p.id
            WHERE s.faixa_probabilidade IN ('janela_quente','provavel')
            GROUP BY p.regiao_imea ORDER BY total DESC
        """)
        return df.fillna("").to_dict(orient="records")
    except Exception as e:
        logger.error(f"regioes: {e}")
        return []


# ── Peritos ────────────────────────────────────────────────────────────────
@app.get("/api/peritos")
async def peritos(
    regiao: Optional[str] = Query(None),
    busca:  Optional[str] = Query(None),
    _user:  AuthUser = None,
):
    try:
        w, p = [], {}
        if regiao: w.append("regiao_imea = :regiao"); p["regiao"] = regiao
        if busca:  w.append("(nome ILIKE :b OR registro_profissional ILIKE :b)"); p["b"] = f"%{busca}%"
        where = ("WHERE " + " AND ".join(w)) if w else ""
        df = _db.query(
            f"SELECT * FROM peritos_agronomos {where} ORDER BY score_profissional DESC NULLS LAST",
            p,
        )
        return {"total": len(df), "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"peritos: {e}")
        return {"total": 0, "items": []}


class PeritoInput(BaseModel):
    nome:                  str
    registro_profissional: str = ""
    especialidades:        str = ""
    municipios_atuacao:    str = ""
    regiao_imea:           str = ""


@app.post("/api/peritos")
async def criar_perito(body: PeritoInput, _user: AuthUser = None):
    try:
        dados = body.model_dump()
        pid = _db.criar_perito(dados)
        return {"id": pid, "status": "created"}
    except Exception as e:
        logger.error(f"criar_perito: {e}")
        raise HTTPException(500, str(e))


# ── Alertas ────────────────────────────────────────────────────────────────
@app.get("/api/alertas")
async def alertas(
    limit: int = Query(40, le=200),
    _user: AuthUser = None,
):
    try:
        df = _db.query("""
            SELECT titulo, resumo, data_publicacao::text AS data_publicacao,
                   municipio, area_ha, fonte, orgao, url,
                   score_evento, faixa_probabilidade,
                   coletado_em::text AS coletado_em
            FROM portarias_diario_oficial
            WHERE faixa_probabilidade IN ('janela_quente','provavel')
            ORDER BY coletado_em DESC
            LIMIT :limit
        """, {"limit": limit})
        return {"total": len(df), "items": df.fillna("").to_dict(orient="records")}
    except Exception as e:
        logger.error(f"alertas: {e}")
        return {"total": 0, "items": []}
