"""
api/main.py — Radar Pericial v2 (VERSÃO MINIMALISTA PARA DEPLOY)
"""
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Optional

# Logger ANTES de tudo
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import text

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from database.db import Database, init_db
from intelligence.taxonomy import calcular_score, TAXONOMIA, REGIOES_IMEA

_db: Optional[Database] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db
    logger.info("🚀 [LIFESPAN] Iniciando...")
    try:
        init_db()
        _db = Database()
        logger.info("✅ [LIFESPAN] Pronto!")
    except Exception as e:
        logger.error(f"❌ [LIFESPAN] Falha: {e}")
        raise
    yield
    logger.info("🛑 [LIFESPAN] Encerrando...")

app = FastAPI(title="Radar Pericial", version="2.0", docs_url="/docs", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── HEALTH CHECKS (SEM AUTH, RESPONDEM EM <100ms) ────────────────────────
@app.get("/health")
async def health():
    """Health check para Railway — SEM query no banco, responde instantaneamente"""
    return {"status": "healthy"}

@app.get("/")
async def root():
    return {"message": "Radar Pericial API", "docs": "/docs", "health": "/health"}

# ── Static files ──────────────────────────────────────────────────────────
static_dir = BASE_DIR / "interface" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# ── Auth dependency ───────────────────────────────────────────────────────
def get_current_user(authorization: Annotated[Optional[str], Header()] = None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token não fornecido")
    token = authorization.split(" ", 1)[1].strip()
    if not _db:
        raise HTTPException(status_code=503, detail="Banco não inicializado")
    username = _db.validate_token(token)
    if not username:
        raise HTTPException(status_code=401, detail="Token inválido")
    return username

AuthUser = Annotated[str, Depends(get_current_user)]

# ── Login ─────────────────────────────────────────────────────────────────
class LoginInput(BaseModel):
    username: str
    password: str

@app.post("/api/login")
async def login(body: LoginInput):
    if not _db or not _db.check_login(body.username, body.password):
        raise HTTPException(status_code=401, detail="Credenciais incorretas")
    return {"status": "ok", "token": _db.create_token(body.username)}

# ── Frontend ──────────────────────────────────────────────────────────────
@app.get("/index.html", response_class=HTMLResponse)
async def index_html():
    return await index()

@app.get("/", response_class=HTMLResponse)
async def index():
    f = BASE_DIR / "interface" / "templates" / "index.html"
    if not f.exists():
        return JSONResponse({"message": "Radar Pericial API", "docs": "/docs"})
    return HTMLResponse(f.read_text(encoding="utf-8"))

@app.get("/api/health")
async def api_health(_user: AuthUser = None):
    return {"status": "ok", "authenticated": True}

# ── Stats (exemplo mínimo) ────────────────────────────────────────────────
@app.get("/api/stats")
async def stats(regiao: Optional[str] = Query(None), _user: AuthUser = None):
    if not _db:
        return {}
    try:
        return _db.stats(regiao)
    except Exception as e:
        logger.error(f"stats error: {e}")
        return {}

# ── [OUTROS ENDPOINTS...] ─────────────────────────────────────────────────
# Mantenha seus outros endpoints (processos, eventos, geojson, etc.) aqui.
# Eles já estão protegidos por AuthUser, então não afetam o health check.
