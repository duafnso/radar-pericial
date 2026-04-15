"""
database/db.py — PostGIS completo para o Radar Pericial
"""

# ── IMPORTS OBRIGATÓRIOS ─────────────────────────────────────────────
# ── IMPORTS OBRIGATÓRIOS ─────────────────────────────────────────────
import logging
import os
from typing import Optional

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)

# ── DEBUG: confirmar carregamento ────────────────────────────────────
logger.info("🔍 db.py está sendo carregado...")

# ── Defaults para variáveis de conexão (evita NameError) ─────────────
host = os.getenv("PGHOST", "localhost")
port = os.getenv("PGPORT", "5432")
user = os.getenv("PGUSER", "postgres")
password = os.getenv("PGPASSWORD", "")
database = os.getenv("PGDATABASE", "radar_pericial")

# ── CONEXÃO COM BANCO (Railway-compatible) ───────────────────────────
_raw_db_url = os.getenv("DATABASE_URL")

if not _raw_db_url:
    # Monta URL com variáveis PG* do Railway
    password_encoded = quote_plus(password)
    _raw_db_url = f"postgresql://{user}:{password_encoded}@{host}:{port}/{database}"
    logger.info(f"🔗 DATABASE_URL montada: {host}:{port}/{database}")
else:
    logger.info("🔗 DATABASE_URL fornecida via variável de ambiente")

# Garante driver psycopg2 para SQLAlchemy 2.0
if _raw_db_url.startswith("postgresql://"):
    DATABASE_URL = _raw_db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
else:
    DATABASE_URL = _raw_db_url

# ── Singleton do engine ──────────────────────────────────────────────
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
        )
    return _engine

# ── CryptContext com fallback seguro ─────────────────────────────────
def _get_pwd_context():
    """Retorna CryptContext com import local + fallback"""
    try:
        from passlib.context import CryptContext
        return CryptContext(schemes=["bcrypt"], deprecated="auto")
    except ImportError as e:
        logger.warning(f"⚠️ passlib não disponível: {e}. Usando fallback SHA256.")
        return None

_pwd_context_ref = _get_pwd_context()

LAYER_TABLE = {
    "municipios_mt": "municipios_mt",
    "limite_estado": "limite_estado_mt",
    "sigef_parcelas": "parcelas_sigef",
    "assentamentos": "assentamentos_incra",
    "desmatamento": "inpe_prodes",
    "alertas_deter": "inpe_deter",
    "car": "cadastro_ambiental",
}

_REFERENCE_LAYERS = {"municipios_mt", "limite_estado_mt"}


class Database:
    def __init__(self, url: str = None):
        if url:
            self.engine = create_engine(url, pool_pre_ping=True)
        else:
            self.engine = get_engine()

    def _init_schema(self):
        """Cria extensões e tabelas."""
        sql = """
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS pg_trgm;

        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            regiao_foco TEXT,
            token TEXT,
            token_expira TIMESTAMPTZ,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS token TEXT;
        ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS token_expira TIMESTAMPTZ;

        CREATE TABLE IF NOT EXISTS municipios_mt (
            id SERIAL PRIMARY KEY,
            codigo_ibge TEXT, nome TEXT,
            regiao_imea TEXT, microrregiao TEXT, mesorregiao TEXT,
            prioridade_monitoramento INT DEFAULT 1,
            fonte TEXT DEFAULT 'IBGE',
            geometry GEOMETRY(MULTIPOLYGON,4326)
        );
        CREATE INDEX IF NOT EXISTS idx_mun_geom ON municipios_mt USING GIST(geometry);

        CREATE TABLE IF NOT EXISTS limite_estado_mt (
            id SERIAL PRIMARY KEY, nome TEXT DEFAULT 'Mato Grosso',
            geometry GEOMETRY(MULTIPOLYGON,4326)
        );

        CREATE TABLE IF NOT EXISTS parcelas_sigef (
            id SERIAL PRIMARY KEY,
            codigo_imovel TEXT, municipio TEXT, area_ha NUMERIC,
            situacao TEXT, desapropriacao_flag BOOLEAN DEFAULT FALSE,
            tipo_camada TEXT DEFAULT 'parcela_rural',
            fonte TEXT DEFAULT 'INCRA/SIGEF',
            coletado_em TIMESTAMPTZ DEFAULT NOW(),
            geometry GEOMETRY(GEOMETRY,4326)
        );
        CREATE INDEX IF NOT EXISTS idx_sigef_geom ON parcelas_sigef USING GIST(geometry);
        CREATE INDEX IF NOT EXISTS idx_sigef_flag ON parcelas_sigef(desapropriacao_flag);
        CREATE INDEX IF NOT EXISTS idx_sigef_mun ON parcelas_sigef(municipio);

        CREATE TABLE IF NOT EXISTS desapropriacao_ativa (
            id SERIAL PRIMARY KEY,
            codigo_imovel TEXT, municipio TEXT, area_ha NUMERIC,
            situacao TEXT, fonte TEXT,
            detectado_em TIMESTAMPTZ DEFAULT NOW(),
            geometry GEOMETRY(GEOMETRY,4326)
        );
        CREATE INDEX IF NOT EXISTS idx_da_geom ON desapropriacao_ativa USING GIST(geometry);

        CREATE TABLE IF NOT EXISTS assentamentos_incra (
            id SERIAL PRIMARY KEY,
            nome_pa TEXT, municipio TEXT, area_ha NUMERIC,
            num_familias INT, fase TEXT,
            fonte TEXT DEFAULT 'INCRA/Assentamentos',
            coletado_em TIMESTAMPTZ DEFAULT NOW(),
            geometry GEOMETRY(GEOMETRY,4326)
        );
        CREATE INDEX IF NOT EXISTS idx_ass_geom ON assentamentos_incra USING GIST(geometry);

        CREATE TABLE IF NOT EXISTS inpe_prodes (
            id SERIAL PRIMARY KEY,
            ano INT, estado TEXT, area_km2 NUMERIC, classe TEXT,
            fonte TEXT DEFAULT 'INPE/PRODES',
            coletado_em TIMESTAMPTZ DEFAULT NOW(),
            geometry GEOMETRY(GEOMETRY,4326)
        );
        CREATE INDEX IF NOT EXISTS idx_prodes_geom ON inpe_prodes USING GIST(geometry);

        CREATE TABLE IF NOT EXISTS inpe_deter (
            id SERIAL PRIMARY KEY,
            view_date DATE, classname TEXT, state TEXT, area_km2 NUMERIC,
            fonte TEXT DEFAULT 'INPE/DETER',
            coletado_em TIMESTAMPTZ DEFAULT NOW(),
            geometry GEOMETRY(GEOMETRY,4326)
        );
        CREATE INDEX IF NOT EXISTS idx_deter_geom ON inpe_deter USING GIST(geometry);

        CREATE TABLE IF NOT EXISTS cadastro_ambiental (
            id SERIAL PRIMARY KEY,
            cod_imovel TEXT, municipio TEXT, area_ha NUMERIC, situacao TEXT,
            fonte TEXT DEFAULT 'CAR/SICAR',
            coletado_em TIMESTAMPTZ DEFAULT NOW(),
            geometry GEOMETRY(GEOMETRY,4326)
        );
        CREATE INDEX IF NOT EXISTS idx_car_geom ON cadastro_ambiental USING GIST(geometry);

        CREATE TABLE IF NOT EXISTS processos (
            id SERIAL PRIMARY KEY,
            numero_cnj TEXT UNIQUE, tribunal TEXT, comarca TEXT, vara TEXT,
            classe_processual TEXT, assunto_principal TEXT,
            data_distribuicao DATE, fase_atual TEXT,
            origem TEXT, municipio TEXT, regiao_imea TEXT,
            ativo BOOLEAN DEFAULT TRUE,
            criado_em TIMESTAMPTZ DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_proc_cnj ON processos(numero_cnj);
        CREATE INDEX IF NOT EXISTS idx_proc_mun ON processos(municipio);

        CREATE TABLE IF NOT EXISTS movimentacoes (
            id SERIAL PRIMARY KEY,
            processo_id INT REFERENCES processos(id) ON DELETE CASCADE,
            data_movimentacao DATE, descricao TEXT,
            fonte TEXT, score_evento INT DEFAULT 0,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_mov_proc ON movimentacoes(processo_id);

        CREATE TABLE IF NOT EXISTS publicacoes (
            id SERIAL PRIMARY KEY,
            processo_id INT REFERENCES processos(id) ON DELETE SET NULL,
            data_publicacao DATE, texto TEXT,
            tipo_publicacao TEXT, palavras_detectadas TEXT,
            orgao_origem TEXT, fonte TEXT, url TEXT,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_pub_proc ON publicacoes(processo_id);

        CREATE TABLE IF NOT EXISTS eventos_administrativos (
            id SERIAL PRIMARY KEY,
            orgao TEXT, data_evento DATE, municipio TEXT, estado TEXT DEFAULT 'MT',
            descricao TEXT, categoria TEXT, score_evento INT DEFAULT 0,
            fonte TEXT, url TEXT, area_ha NUMERIC,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_ev_mun ON eventos_administrativos(municipio);

        CREATE TABLE IF NOT EXISTS score_pericial (
            id SERIAL PRIMARY KEY,
            processo_id INT REFERENCES processos(id) ON DELETE CASCADE,
            score_total INT DEFAULT 0,
            score_classe INT DEFAULT 0, score_assunto INT DEFAULT 0,
            score_movimentacao INT DEFAULT 0, score_publicacao INT DEFAULT 0,
            score_administrativo INT DEFAULT 0,
            faixa_probabilidade TEXT DEFAULT 'frio',
            faixa_label TEXT DEFAULT '❄️ Frio',
            tipo_pericia_sugerida TEXT, categorias_detectadas TEXT,
            urgencia TEXT DEFAULT 'baixa',
            calculado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_score_proc ON score_pericial(processo_id);
        CREATE INDEX IF NOT EXISTS idx_score_total ON score_pericial(score_total DESC);
        CREATE INDEX IF NOT EXISTS idx_score_faixa ON score_pericial(faixa_probabilidade);

        CREATE TABLE IF NOT EXISTS peritos_agronomos (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL, registro_profissional TEXT,
            especialidades TEXT, municipios_atuacao TEXT,
            regiao_imea TEXT, perfil_publico BOOLEAN DEFAULT TRUE,
            score_profissional INT DEFAULT 0,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS portarias_diario_oficial (
            id SERIAL PRIMARY KEY,
            titulo TEXT, resumo TEXT, data_publicacao TEXT,
            municipio TEXT, area_ha NUMERIC, fonte TEXT, orgao TEXT,
            url TEXT, categoria_agronomica TEXT,
            score_evento INT DEFAULT 0,
            faixa_probabilidade TEXT DEFAULT 'frio',
            coletado_em TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS data_lake_raw (
            id SERIAL PRIMARY KEY,
            fonte TEXT, tipo TEXT,
            payload JSONB, processado BOOLEAN DEFAULT FALSE,
            coletado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_raw_proc ON data_lake_raw(processado);
        """
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            import hashlib
            h = hashlib.sha256("admin".encode()).hexdigest()
            conn.execute(text(
                "INSERT INTO usuarios (username, password_hash) "
                "VALUES ('admin', :h) ON CONFLICT (username) DO NOTHING"
            ), {"h": h})
            conn.commit()
        logger.info("✅ Schema inicializado.")

    def check_login(self, username: str, password_raw: str) -> bool:
        # Obtém pwd_context com fallback
        pwd_ctx = _get_pwd_context()
        
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id, password_hash FROM usuarios WHERE username=:u"),
                {"u": username},
            ).fetchone()
            if not row:
                return False
            uid, stored = row[0], row[1]

            # Tenta bcrypt primeiro
            if stored.startswith("$2b$") or stored.startswith("$2a$"):
                if pwd_ctx:
                    return pwd_ctx.verify(password_raw, stored)
                logger.warning("bcrypt hash mas passlib não disponível")
                return False

            # Fallback SHA256 legado
            import hashlib
            if hashlib.sha256(password_raw.encode()).hexdigest() == stored:
                # Tenta upgrade para bcrypt
                if pwd_ctx:
                    new_hash = pwd_ctx.hash(password_raw)
                    conn.execute(
                        text("UPDATE usuarios SET password_hash=:h WHERE id=:id"),
                        {"h": new_hash, "id": uid},
                    )
                    conn.commit()
                    logger.info(f"🔐 Senha de '{username}' upgradada para bcrypt.")
                return True
            return False

    def create_token(self, username: str) -> str:
        import uuid
        from datetime import datetime, timedelta
        token = str(uuid.uuid4())
        expira = datetime.utcnow() + timedelta(hours=24)
        with self.engine.connect() as conn:
            conn.execute(
                text("UPDATE usuarios SET token=:t, token_expira=:e WHERE username=:u"),
                {"t": token, "e": expira, "u": username},
            )
            conn.commit()
        return token

    def validate_token(self, token: str) -> Optional[str]:
        if not token:
            return None
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT username FROM usuarios WHERE token=:t AND token_expira > NOW()"),
                {"t": token},
            ).fetchone()
            return row[0] if row else None

    def save_geodataframe(self, gdf, table: str, if_exists: str = "append"):
        if gdf is None or (hasattr(gdf, "empty") and gdf.empty):
            logger.warning(f"GDF vazio → '{table}' ignorado")
            return
        if not isinstance(gdf, gpd.GeoDataFrame):
            try:
                pd.DataFrame(gdf).to_sql(table, self.engine, if_exists=if_exists, index=False)
            except Exception as e:
                logger.error(f"Erro tabela não-geo '{table}': {e}")
            return
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326)
        elif gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        try:
            gdf.to_postgis(table, self.engine, if_exists=if_exists, index=False)
            logger.info(f"'{table}': {len(gdf)} registros salvos")
        except Exception as e:
            logger.error(f"Erro '{table}': {e}")

    def _upsert_sigef(self, gdf: gpd.GeoDataFrame):
        if gdf is None or gdf.empty:
            return
        staging = "parcelas_sigef_staging"
        try:
            self.save_geodataframe(gdf, staging, if_exists="replace")
            with self.engine.connect() as conn:
                conn.execute(text("""
                    DELETE FROM parcelas_sigef
                    WHERE codigo_imovel IN (
                        SELECT codigo_imovel FROM parcelas_sigef_staging
                        WHERE codigo_imovel IS NOT NULL
                    )
                """))
                conn.execute(text("""
                    INSERT INTO parcelas_sigef
                        (codigo_imovel, municipio, area_ha, situacao,
                         desapropriacao_flag, tipo_camada, fonte, coletado_em, geometry)
                    SELECT codigo_imovel, municipio, area_ha, situacao,
                           desapropriacao_flag, tipo_camada, fonte,
                           COALESCE(coletado_em::timestamptz, NOW()), geometry
                    FROM parcelas_sigef_staging
                """))
                conn.execute(text("DROP TABLE IF EXISTS parcelas_sigef_staging"))
                conn.commit()
            logger.info(f"SIGEF upsert: {len(gdf)} parcelas processadas")
        except Exception as e:
            logger.error(f"SIGEF upsert falhou ({e}) — fallback append")
            with self.engine.connect() as conn:
                try:
                    conn.execute(text("DROP TABLE IF EXISTS parcelas_sigef_staging"))
                    conn.commit()
                except Exception:
                    pass
            self.save_geodataframe(gdf, "parcelas_sigef", if_exists="append")

    def save_all_layers(self, layers: dict):
        for k, v in layers.items():
            table = LAYER_TABLE.get(k, k)
            if v is None or (hasattr(v, "empty") and v.empty):
                logger.info(f"'{table}': layer vazio, preservado")
                continue
            if table in _REFERENCE_LAYERS:
                self.save_geodataframe(v, table, if_exists="replace")
            elif table == "parcelas_sigef" and isinstance(v, gpd.GeoDataFrame) and "codigo_imovel" in v.columns:
                self._upsert_sigef(v)
            else:
                self.save_geodataframe(v, table, if_exists="append")

    def upsert_processo(self, dados: dict) -> Optional[int]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM processos WHERE numero_cnj = :cnj"),
                {"cnj": dados.get("numero_cnj")},
            ).fetchone()
            if row:
                conn.execute(
                    text("""
                        UPDATE processos SET tribunal = COALESCE(NULLIF(:tribunal, ''), tribunal),
                            comarca = COALESCE(NULLIF(:comarca, ''), comarca),
                            vara = COALESCE(NULLIF(:vara, ''), vara),
                            classe_processual = COALESCE(NULLIF(:classe_processual, ''), classe_processual),
                            assunto_principal = COALESCE(NULLIF(:assunto_principal, ''), assunto_principal),
                            data_distribuicao = COALESCE(:data_distribuicao, data_distribuicao),
                            fase_atual = COALESCE(NULLIF(:fase_atual, ''), fase_atual),
                            municipio = COALESCE(NULLIF(:municipio, ''), municipio),
                            regiao_imea = COALESCE(NULLIF(:regiao_imea, ''), regiao_imea),
                            atualizado_em = NOW()
                        WHERE id = :id
                    """),
                    {
                        "id": row[0],
                        "tribunal": dados.get("tribunal", ""),
                        "comarca": dados.get("comarca", ""),
                        "vara": dados.get("vara", ""),
                        "classe_processual": dados.get("classe_processual", ""),
                        "assunto_principal": dados.get("assunto_principal", ""),
                        "data_distribuicao": dados.get("data_distribuicao") or None,
                        "fase_atual": dados.get("fase_atual", ""),
                        "municipio": dados.get("municipio", ""),
                        "regiao_imea": dados.get("regiao_imea", ""),
                    },
                )
                conn.commit()
                return row[0]
            campos = ["numero_cnj","tribunal","comarca","vara","classe_processual",
                      "assunto_principal","data_distribuicao","fase_atual",
                      "origem","municipio","regiao_imea"]
            vals = {c: dados.get(c) for c in campos}
            r = conn.execute(
                text(f"INSERT INTO processos ({','.join(campos)}) VALUES ({','.join(':'+c for c in campos)}) RETURNING id"),
                vals,
            )
            pid = r.fetchone()[0]
            conn.commit()
            return pid

    def save_score(self, processo_id: int, score_dict: dict):
        with self.engine.connect() as conn:
            conn.execute(text("DELETE FROM score_pericial WHERE processo_id=:id"), {"id": processo_id})
            campos = ["processo_id","score_total","score_classe","score_assunto",
                      "score_movimentacao","score_publicacao","score_administrativo",
                      "faixa_probabilidade","faixa_label","tipo_pericia_sugerida",
                      "categorias_detectadas","urgencia"]
            vals = {c: score_dict.get(c) for c in campos}
            vals["processo_id"] = processo_id
            conn.execute(
                text(f"INSERT INTO score_pericial ({','.join(campos)}) VALUES ({','.join(':'+c for c in campos)})"),
                vals,
            )
            conn.commit()

    def save_movimentacao(self, processo_id: int, dados: dict):
        with self.engine.connect() as conn:
            existing = conn.execute(
                text("""
                    SELECT id FROM movimentacoes
                    WHERE processo_id = :pid AND descricao = :desc
                    AND (data_movimentacao = :dt OR (data_movimentacao IS NULL AND :dt IS NULL))
                    LIMIT 1
                """),
                {"pid": processo_id, "desc": dados.get("descricao"), "dt": dados.get("data_movimentacao")},
            ).fetchone()
            if existing:
                logger.debug(f"Movimentação duplicada ignorada: processo {processo_id}")
                return
            conn.execute(
                text("""
                    INSERT INTO movimentacoes (processo_id, data_movimentacao, descricao, fonte, score_evento)
                    VALUES (:pid, :dt, :desc, :fonte, :score)
                """),
                {
                    "pid": processo_id,
                    "dt": dados.get("data_movimentacao"),
                    "desc": dados.get("descricao"),
                    "fonte": dados.get("fonte", ""),
                    "score": dados.get("score_evento", 0),
                },
            )
            conn.commit()

    def save_portarias(self, portarias: list):
        if not portarias:
            return
        df_new = pd.DataFrame(portarias)
        def _chave(row) -> str:
            return str(row.get("titulo", "") or "") + "|" + str(row.get("data_publicacao", "") or "") + "|" + str(row.get("fonte", "") or "")
        try:
            existing = self.query("SELECT titulo, data_publicacao::text AS data_publicacao, fonte FROM portarias_diario_oficial")
            if not existing.empty:
                existing_keys = set(existing.apply(_chave, axis=1))
                df_new["_key"] = df_new.apply(_chave, axis=1)
                df_new = df_new[~df_new["_key"].isin(existing_keys)].drop(columns=["_key"])
        except Exception as e:
            logger.warning(f"Dedup portarias: {e}")
        if df_new.empty:
            logger.info("Portarias: nenhuma nova")
            return
        df_new.to_sql("portarias_diario_oficial", self.engine, if_exists="append", index=False)
        logger.info(f"Portarias: {len(df_new)} novas salvas")

    def criar_perito(self, dados: dict) -> int:
        with self.engine.connect() as conn:
            r = conn.execute(
                text("""
                    INSERT INTO peritos_agronomos (nome, registro_profissional, especialidades, municipios_atuacao, regiao_imea)
                    VALUES (:nome, :registro, :especialidades, :municipios, :regiao) RETURNING id
                """),
                {
                    "nome": dados.get("nome"),
                    "registro": dados.get("registro_profissional", ""),
                    "especialidades": dados.get("especialidades", ""),
                    "municipios": dados.get("municipios_atuacao", ""),
                    "regiao": dados.get("regiao_imea", ""),
                },
            )
            pid = r.fetchone()[0]
            conn.commit()
            return pid

    def query(self, sql: str, params: dict = None) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn, params=params or {})

    def query_geo(self, sql: str) -> gpd.GeoDataFrame:
        return gpd.read_postgis(sql, self.engine, geom_col="geometry")

    def get_layers_for_map(self) -> dict:
        tables = {
            "municipios": "SELECT codigo_ibge, nome, regiao_imea, geometry FROM municipios_mt",
            "parcelas": "SELECT codigo_imovel, municipio, area_ha, situacao, desapropriacao_flag, fonte, geometry FROM parcelas_sigef",
            "assentamentos": "SELECT nome_pa, municipio, area_ha, num_familias, fase, geometry FROM assentamentos_incra",
            "prodes": "SELECT ano, area_km2, classe, geometry FROM inpe_prodes",
            "deter": "SELECT view_date, classname, area_km2, geometry FROM inpe_deter",
            "car": "SELECT cod_imovel, municipio, area_ha, situacao, geometry FROM cadastro_ambiental",
        }
        result = {}
        for key, sql in tables.items():
            try:
                result[key] = self.query_geo(sql)
            except Exception:
                result[key] = gpd.GeoDataFrame()
        return result

    def get_processos_quentes(self, faixa: str = "janela_quente", limit: int = 100) -> pd.DataFrame:
        return self.query(
            """
            SELECT p.numero_cnj, p.tribunal, p.comarca, p.classe_processual,
                   p.municipio, p.regiao_imea,
                   s.score_total, s.faixa_probabilidade, s.faixa_label,
                   s.tipo_pericia_sugerida, s.urgencia
            FROM processos p JOIN score_pericial s ON s.processo_id = p.id
            WHERE s.faixa_probabilidade = :faixa
            ORDER BY s.score_total DESC LIMIT :limit
            """,
            {"faixa": faixa, "limit": limit},
        )

    def get_eventos_recentes(self, limit: int = 100) -> pd.DataFrame:
        return self.query(
            """
            SELECT orgao, data_publicacao, municipio, titulo, resumo,
                   score_evento, faixa_probabilidade, fonte, url
            FROM portarias_diario_oficial
            ORDER BY coletado_em DESC LIMIT :limit
            """,
            {"limit": limit},
        )

    def stats(self, regiao: Optional[str] = None) -> dict:
        mun_f = desap_f = port_f = assent_f = proc_f = ""
        sc_f = "WHERE faixa_probabilidade="
        if regiao:
            mun_f = f"WHERE municipio IN (SELECT nome FROM municipios_mt WHERE regiao_imea = '{regiao}')"
            desap_f = f"WHERE desapropriacao_flag=TRUE AND municipio IN (SELECT nome FROM municipios_mt WHERE regiao_imea = '{regiao}')"
            port_f = f"WHERE municipio IN (SELECT nome FROM municipios_mt WHERE regiao_imea = '{regiao}')"
            assent_f = f"WHERE municipio IN (SELECT nome FROM municipios_mt WHERE regiao_imea = '{regiao}')"
            proc_f = f"WHERE regiao_imea = '{regiao}'"
            sc_f = f"JOIN processos p ON score_pericial.processo_id = p.id WHERE p.regiao_imea = '{regiao}' AND faixa_probabilidade="
        qs = {
            "total_parcelas": f"SELECT COUNT(*) FROM parcelas_sigef {mun_f}",
            "total_desapropriadas": f"SELECT COUNT(*) FROM parcelas_sigef {desap_f}",
            "area_total_ha": f"SELECT COALESCE(SUM(area_ha),0) FROM desapropriacao_ativa {mun_f}",
            "total_portarias": f"SELECT COUNT(*) FROM portarias_diario_oficial {port_f}",
            "total_assentamentos": f"SELECT COUNT(*) FROM assentamentos_incra {assent_f}",
            "total_alertas_deter": "SELECT COUNT(*) FROM inpe_deter",
            "total_processos": f"SELECT COUNT(*) FROM processos {proc_f}",
            "processos_quentes": f"SELECT COUNT(*) FROM score_pericial {sc_f}'janela_quente'",
            "processos_provaveis": f"SELECT COUNT(*) FROM score_pericial {sc_f}'provavel'",
            "ultima_coleta": "SELECT MAX(coletado_em) FROM portarias_diario_oficial",
        }
        result = {}
        for k, s in qs.items():
            try:
                result[k] = self.query(s).iloc[0, 0]
            except Exception:
                result[k] = 0
        return result


def init_db():
    db = Database()
    db._init_schema()
    logger.info("🎉 Banco de dados pronto.")
