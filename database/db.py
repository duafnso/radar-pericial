"""
database/db.py — PostGIS completo para o Radar Pericial
"""
# ── IMPORTS OBRIGATÓRIOS ─────────────────────────────────────────────
import logging
import os
import hashlib
import hmac
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


def json_dumps(value: dict) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)

# ── DEBUG: confirmar carregamento ────────────────────────────────────
logger.info("database/db.py carregado.")

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
    logger.info("DATABASE_URL montada: %s:%s/%s", host, port, database)
else:
    logger.info("DATABASE_URL fornecida via variavel de ambiente.")

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
    """Retorna CryptContext usando esquemas seguros."""
    try:
        from passlib.context import CryptContext
        return CryptContext(schemes=["argon2", "bcrypt"], deprecated="auto")
    except ImportError as e:
        raise RuntimeError(
            "passlib é obrigatório para autenticação segura (bcrypt/argon2)."
        ) from e

_pwd_context_ref = _get_pwd_context()


def _is_production() -> bool:
    return os.getenv("APP_ENV", os.getenv("ENV", "development")).strip().lower() in {
        "prod",
        "production",
    }


def _load_session_token_pepper() -> str:
    pepper = os.getenv("SESSION_TOKEN_PEPPER") or os.getenv("SECRET_KEY")
    if pepper:
        return pepper
    if _is_production():
        raise RuntimeError(
            "SESSION_TOKEN_PEPPER ou SECRET_KEY deve ser definido em produção."
        )
    logger.warning(
        "SESSION_TOKEN_PEPPER/SECRET_KEY ausente; usando pepper de desenvolvimento."
    )
    return "dev-token-pepper"


_SESSION_TOKEN_PEPPER = _load_session_token_pepper()


def _hash_session_token(token: str) -> str:
    return hmac.new(_SESSION_TOKEN_PEPPER.encode(), token.encode(), hashlib.sha256).hexdigest()

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

_GEO_UPSERT_KEYS = {
    "assentamentos_incra": ["nome_pa", "municipio"],
    "inpe_prodes": ["ano", "classe"],
    "inpe_deter": ["view_date", "classname", "state"],
    "cadastro_ambiental": ["cod_imovel"],
    "desapropriacao_ativa": ["codigo_imovel"],
}


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
            role TEXT NOT NULL DEFAULT 'user',
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            regiao_foco TEXT,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        ALTER TABLE usuarios
            ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user';
        ALTER TABLE usuarios
            ADD COLUMN IF NOT EXISTS ativo BOOLEAN NOT NULL DEFAULT TRUE;
        CREATE INDEX IF NOT EXISTS idx_usuarios_role ON usuarios(role);
        CREATE INDEX IF NOT EXISTS idx_usuarios_ativo ON usuarios(ativo);
        UPDATE usuarios SET role = 'admin' WHERE username = 'admin';

        CREATE TABLE IF NOT EXISTS user_sessions (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            token_hash TEXT UNIQUE NOT NULL,
            criado_em TIMESTAMPTZ DEFAULT NOW(),
            expira_em TIMESTAMPTZ NOT NULL,
            revogado_em TIMESTAMPTZ,
            ultimo_uso_em TIMESTAMPTZ,
            user_agent TEXT,
            client_ip TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_exp ON user_sessions(expira_em);

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
        DELETE FROM movimentacoes a
        USING movimentacoes b
        WHERE a.id > b.id
          AND a.processo_id = b.processo_id
          AND COALESCE(a.data_movimentacao, DATE '0001-01-01') = COALESCE(b.data_movimentacao, DATE '0001-01-01')
          AND COALESCE(a.descricao, '') = COALESCE(b.descricao, '');
        CREATE UNIQUE INDEX IF NOT EXISTS ux_mov_unique_business
            ON movimentacoes(
                processo_id,
                (COALESCE(data_movimentacao, DATE '0001-01-01')),
                md5(COALESCE(descricao, ''))
            );

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
        DELETE FROM portarias_diario_oficial a
        USING portarias_diario_oficial b
        WHERE a.id > b.id
          AND COALESCE(a.titulo, '') = COALESCE(b.titulo, '')
          AND COALESCE(a.data_publicacao, '') = COALESCE(b.data_publicacao, '')
          AND COALESCE(a.fonte, '') = COALESCE(b.fonte, '');
        CREATE UNIQUE INDEX IF NOT EXISTS ux_portarias_unique_business
            ON portarias_diario_oficial(
                md5(COALESCE(titulo, '')),
                (COALESCE(data_publicacao, '')),
                (COALESCE(fonte, ''))
            );

        CREATE TABLE IF NOT EXISTS data_lake_raw (
            id SERIAL PRIMARY KEY,
            fonte TEXT, tipo TEXT,
            payload JSONB, processado BOOLEAN DEFAULT FALSE,
            coletado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_raw_proc ON data_lake_raw(processado);

        CREATE TABLE IF NOT EXISTS execucoes_coleta (
            id SERIAL PRIMARY KEY,
            fonte TEXT NOT NULL,
            tarefa TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running',
            parametros JSONB,
            registros_coletados INT DEFAULT 0,
            registros_salvos INT DEFAULT 0,
            erro TEXT,
            iniciado_em TIMESTAMPTZ DEFAULT NOW(),
            finalizado_em TIMESTAMPTZ,
            duracao_segundos NUMERIC
        );
        CREATE INDEX IF NOT EXISTS idx_exec_coleta_fonte
            ON execucoes_coleta(fonte, iniciado_em DESC);
        CREATE INDEX IF NOT EXISTS idx_exec_coleta_status
            ON execucoes_coleta(status, iniciado_em DESC);

        CREATE TABLE IF NOT EXISTS metricas_coleta_classe (
            id SERIAL PRIMARY KEY,
            execucao_id INT REFERENCES execucoes_coleta(id) ON DELETE CASCADE,
            fonte TEXT NOT NULL,
            chave TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'success',
            registros_coletados INT DEFAULT 0,
            registros_salvos INT DEFAULT 0,
            descartados_sem_cnj INT DEFAULT 0,
            duplicados INT DEFAULT 0,
            erro TEXT,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_metricas_coleta_execucao
            ON metricas_coleta_classe(execucao_id, chave);
        CREATE INDEX IF NOT EXISTS idx_metricas_coleta_fonte
            ON metricas_coleta_classe(fonte, criado_em DESC);

        CREATE TABLE IF NOT EXISTS auditoria_eventos (
            id SERIAL PRIMARY KEY,
            ator_user_id INT REFERENCES usuarios(id) ON DELETE SET NULL,
            ator_username TEXT,
            acao TEXT NOT NULL,
            entidade TEXT,
            entidade_id TEXT,
            detalhes JSONB,
            ip TEXT,
            user_agent TEXT,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_auditoria_eventos_criado
            ON auditoria_eventos(criado_em DESC);
        CREATE INDEX IF NOT EXISTS idx_auditoria_eventos_ator
            ON auditoria_eventos(ator_user_id, criado_em DESC);
        CREATE INDEX IF NOT EXISTS idx_auditoria_eventos_acao
            ON auditoria_eventos(acao, criado_em DESC);

        CREATE TABLE IF NOT EXISTS processos_acompanhados (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            processo_id INT NOT NULL REFERENCES processos(id) ON DELETE CASCADE,
            ativo BOOLEAN NOT NULL DEFAULT TRUE,
            criado_em TIMESTAMPTZ DEFAULT NOW(),
            atualizado_em TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, processo_id)
        );
        CREATE INDEX IF NOT EXISTS idx_proc_acomp_user
            ON processos_acompanhados(user_id, ativo, criado_em DESC);

        CREATE TABLE IF NOT EXISTS alertas_usuario (
            id SERIAL PRIMARY KEY,
            user_id INT NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            processo_id INT REFERENCES processos(id) ON DELETE CASCADE,
            tipo TEXT NOT NULL DEFAULT 'processo',
            titulo TEXT NOT NULL,
            mensagem TEXT,
            lido BOOLEAN NOT NULL DEFAULT FALSE,
            criado_em TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_alertas_usuario_user
            ON alertas_usuario(user_id, lido, criado_em DESC);
        """
        lock_key = 83476201
        with self.engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": lock_key})
            conn.execute(text(sql))
            default_admin_password = os.getenv("DEFAULT_ADMIN_PASSWORD")
            if default_admin_password:
                h = _pwd_context_ref.hash(default_admin_password)
                conn.execute(text(
                    "INSERT INTO usuarios (username, password_hash, ativo) "
                    "VALUES ('admin', :h, TRUE) "
                    "ON CONFLICT (username) DO UPDATE "
                    "SET password_hash = EXCLUDED.password_hash, role = 'admin', ativo = TRUE"
                ), {"h": h})
            else:
                logger.warning("DEFAULT_ADMIN_PASSWORD ausente; usuário admin padrão não foi criado.")
            conn.commit()
        logger.info("Schema inicializado.")

    def check_login(self, username: str, password_raw: str) -> bool:
        pwd_ctx = _pwd_context_ref
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT id, password_hash FROM usuarios WHERE username=:u AND ativo = TRUE"),
                {"u": username},
            ).fetchone()
            if not row:
                return False
            stored = row[1]
            if not stored:
                return False
            try:
                return bool(pwd_ctx.verify(password_raw, stored))
            except (ValueError, TypeError):
                logger.warning("Hash de senha inválido para usuário '%s'.", username)
                return False

    def create_token(
        self,
        username: str,
        user_agent: Optional[str] = None,
        client_ip: Optional[str] = None,
        ttl_hours: int = 24,
    ) -> str:
        token = secrets.token_urlsafe(48)
        token_hash = _hash_session_token(token)
        expira = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
        with self.engine.connect() as conn:
            user = conn.execute(
                text("SELECT id FROM usuarios WHERE username=:u AND ativo = TRUE"),
                {"u": username},
            ).fetchone()
            if not user:
                raise ValueError("Usuário inexistente para criação de sessão.")
            conn.execute(
                text("""
                    INSERT INTO user_sessions
                    (user_id, token_hash, expira_em, user_agent, client_ip, ultimo_uso_em)
                    VALUES (:uid, :th, :exp, :ua, :ip, NOW())
                """),
                {
                    "uid": user[0],
                    "th": token_hash,
                    "exp": expira,
                    "ua": user_agent,
                    "ip": client_ip,
                },
            )
            conn.commit()
        return token

    def validate_token(
        self,
        token: str,
        user_agent: Optional[str] = None,
        client_ip: Optional[str] = None,
    ) -> Optional[str]:
        user = self.validate_token_user(token, user_agent=user_agent, client_ip=client_ip)
        return user["username"] if user else None

    def validate_token_user(
        self,
        token: str,
        user_agent: Optional[str] = None,
        client_ip: Optional[str] = None,
    ) -> Optional[dict]:
        if not token:
            return None
        token_hash = _hash_session_token(token)
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT s.id, u.id AS user_id, u.username, COALESCE(u.role, 'user') AS role
                    FROM user_sessions s
                    JOIN usuarios u ON u.id = s.user_id
                    WHERE s.token_hash = :th
                      AND s.revogado_em IS NULL
                      AND s.expira_em > NOW()
                      AND u.ativo = TRUE
                    LIMIT 1
                """),
                {"th": token_hash},
            ).fetchone()
            if not row:
                return None
            conn.execute(
                text("""
                    UPDATE user_sessions
                    SET ultimo_uso_em = NOW(),
                        user_agent = COALESCE(:ua, user_agent),
                        client_ip = COALESCE(:ip, client_ip)
                    WHERE id = :sid
                """),
                {"sid": row[0], "ua": user_agent, "ip": client_ip},
            )
            conn.commit()
            return {"id": row[1], "username": row[2], "role": row[3]}

    def revoke_token(self, token: str) -> bool:
        if not token:
            return False
        token_hash = _hash_session_token(token)
        with self.engine.connect() as conn:
            res = conn.execute(
                text("""
                    UPDATE user_sessions
                    SET revogado_em = NOW()
                    WHERE token_hash = :th AND revogado_em IS NULL
                """),
                {"th": token_hash},
            )
            conn.commit()
            return res.rowcount > 0

    def cleanup_expired_sessions(self, retention_days: int = 7) -> int:
        retention_days = max(0, min(int(retention_days), 365))
        with self.engine.connect() as conn:
            res = conn.execute(
                text("""
                    DELETE FROM user_sessions
                    WHERE expira_em < NOW() - (:retention_days * INTERVAL '1 day')
                       OR (
                           revogado_em IS NOT NULL
                           AND revogado_em < NOW() - (:retention_days * INTERVAL '1 day')
                       )
                """),
                {"retention_days": retention_days},
            )
            conn.commit()
            return max(res.rowcount or 0, 0)

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
            logger.error(f"SIGEF upsert falhou ({e}); dados existentes preservados e append ignorado para evitar duplicidade")
            with self.engine.connect() as conn:
                try:
                    conn.execute(text("DROP TABLE IF EXISTS parcelas_sigef_staging"))
                    conn.commit()
                except Exception:
                    pass

    def _safe_ident(self, name: str) -> str:
        if not name or not name.replace("_", "").isalnum():
            raise ValueError(f"Identificador SQL invalido: {name}")
        return f'"{name}"'

    def _table_columns(self, conn, table: str) -> list[str]:
        rows = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table
                ORDER BY ordinal_position
            """),
            {"table": table},
        ).fetchall()
        return [row[0] for row in rows]

    def _upsert_geolayer(self, gdf: gpd.GeoDataFrame, table: str, key_cols: list[str]):
        if gdf is None or gdf.empty:
            return
        staging = f"{table}_staging"
        try:
            self.save_geodataframe(gdf, staging, if_exists="replace")
            q_table = self._safe_ident(table)
            q_staging = self._safe_ident(staging)

            with self.engine.connect() as conn:
                target_cols = self._table_columns(conn, table)
                staging_cols = self._table_columns(conn, staging)
                insert_cols = [
                    col for col in target_cols
                    if col != "id" and col in staging_cols
                ]
                if "geometry" not in insert_cols:
                    raise ValueError(f"Layer {table} sem coluna geometry compativel")

                usable_keys = [
                    col for col in key_cols
                    if col in target_cols and col in staging_cols
                ]
                conditions = []
                for col in usable_keys:
                    q_col = self._safe_ident(col)
                    conditions.append(
                        f"COALESCE(t.{q_col}::text, '') = COALESCE(s.{q_col}::text, '')"
                    )
                if "geometry" in target_cols and "geometry" in staging_cols:
                    conditions.append(
                        "md5(encode(ST_AsEWKB(t.geometry), 'hex')) = md5(encode(ST_AsEWKB(s.geometry), 'hex'))"
                    )
                if not conditions:
                    raise ValueError(f"Layer {table} sem chave de deduplicacao utilizavel")

                conn.execute(text(f"""
                    DELETE FROM {q_table} t
                    USING {q_staging} s
                    WHERE {' AND '.join(conditions)}
                """))

                q_cols = [self._safe_ident(col) for col in insert_cols]
                conn.execute(text(f"""
                    INSERT INTO {q_table} ({', '.join(q_cols)})
                    SELECT {', '.join(q_cols)}
                    FROM {q_staging}
                """))
                conn.execute(text(f"DROP TABLE IF EXISTS {q_staging}"))
                conn.commit()
            logger.info(f"{table} upsert: {len(gdf)} registros processados")
        except Exception as e:
            logger.error(f"{table} upsert falhou ({e}); dados existentes preservados e append ignorado para evitar duplicidade")
            with self.engine.connect() as conn:
                try:
                    conn.execute(text(f"DROP TABLE IF EXISTS {self._safe_ident(staging)}"))
                    conn.commit()
                except Exception:
                    pass

    def save_desapropriacao_ativa(self, gdf: gpd.GeoDataFrame):
        self._upsert_geolayer(
            gdf,
            "desapropriacao_ativa",
            _GEO_UPSERT_KEYS["desapropriacao_ativa"],
        )

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
            elif table in _GEO_UPSERT_KEYS and isinstance(v, gpd.GeoDataFrame):
                self._upsert_geolayer(v, table, _GEO_UPSERT_KEYS[table])
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
                return False
            conn.execute(
                text("""
                    INSERT INTO movimentacoes (processo_id, data_movimentacao, descricao, fonte, score_evento)
                    VALUES (:pid, :dt, :desc, :fonte, :score)
                    ON CONFLICT DO NOTHING
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
        self.notify_followers_for_processo(
            processo_id,
            "Nova movimentacao em processo acompanhado",
            dados.get("descricao") or "O processo acompanhado recebeu uma nova movimentacao.",
        )
        return True

    def save_portarias(self, portarias: list):
        if not portarias:
            return
        df_new = pd.DataFrame(portarias)
        allowed = [
            "titulo", "resumo", "data_publicacao", "municipio", "area_ha",
            "fonte", "orgao", "url", "categoria_agronomica",
            "score_evento", "faixa_probabilidade",
        ]
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
        def _clean_scalar(value):
            if isinstance(value, (list, dict, tuple, set)):
                return json_dumps(value) if isinstance(value, dict) else json.dumps(list(value), ensure_ascii=False, default=str)
            return None if pd.isna(value) else value
        rows = []
        for row in df_new.to_dict(orient="records"):
            clean = {}
            for col in allowed:
                value = row.get(col)
                clean[col] = _clean_scalar(value)
            rows.append(clean)
        if not rows:
            logger.info("Portarias: nenhuma nova")
            return
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO portarias_diario_oficial (
                        titulo, resumo, data_publicacao, municipio, area_ha,
                        fonte, orgao, url, categoria_agronomica,
                        score_evento, faixa_probabilidade
                    )
                    VALUES (
                        :titulo, :resumo, :data_publicacao, :municipio, :area_ha,
                        :fonte, :orgao, :url, :categoria_agronomica,
                        COALESCE(:score_evento, 0),
                        COALESCE(:faixa_probabilidade, 'frio')
                    )
                    ON CONFLICT DO NOTHING
                """),
                rows,
            )
            conn.commit()
        salvas = max(result.rowcount, 0)
        logger.info(f"Portarias: {salvas} novas salvas")

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

    def listar_usuarios(self) -> pd.DataFrame:
        return self.query("""
            SELECT id, username, role, ativo, regiao_foco,
                   criado_em::text AS criado_em
            FROM usuarios
            ORDER BY id ASC
        """)

    def criar_usuario(
        self,
        username: str,
        password_raw: str,
        role: str = "user",
        regiao_foco: Optional[str] = None,
    ) -> int:
        role = role if role in {"admin", "user", "viewer", "operator"} else "user"
        password_hash = _pwd_context_ref.hash(password_raw)
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO usuarios (username, password_hash, role, regiao_foco, ativo)
                    VALUES (:username, :password_hash, :role, :regiao_foco, TRUE)
                    ON CONFLICT (username) DO NOTHING
                    RETURNING id
                """),
                {
                    "username": username,
                    "password_hash": password_hash,
                    "role": role,
                    "regiao_foco": regiao_foco,
                },
            ).fetchone()
            if not row:
                conn.rollback()
                raise ValueError("Username ja existe.")
            conn.commit()
            return int(row[0])

    def atualizar_role_usuario(self, user_id: int, role: str) -> bool:
        if role not in {"admin", "user", "viewer", "operator"}:
            raise ValueError("Role invalida.")
        with self.engine.connect() as conn:
            result = conn.execute(
                text("UPDATE usuarios SET role=:role WHERE id=:id"),
                {"id": user_id, "role": role},
            )
            conn.commit()
            return result.rowcount > 0

    def definir_usuario_ativo(self, user_id: int, ativo: bool) -> bool:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("UPDATE usuarios SET ativo=:ativo WHERE id=:id"),
                {"id": user_id, "ativo": ativo},
            )
            if result.rowcount:
                conn.execute(
                    text("""
                        UPDATE user_sessions
                        SET revogado_em = NOW()
                        WHERE user_id = :id AND revogado_em IS NULL
                    """),
                    {"id": user_id},
                )
            conn.commit()
            return result.rowcount > 0

    def redefinir_senha_usuario(self, user_id: int, password_raw: str) -> bool:
        password_hash = _pwd_context_ref.hash(password_raw)
        with self.engine.connect() as conn:
            result = conn.execute(
                text("UPDATE usuarios SET password_hash=:password_hash WHERE id=:id"),
                {"id": user_id, "password_hash": password_hash},
            )
            if result.rowcount:
                conn.execute(
                    text("""
                        UPDATE user_sessions
                        SET revogado_em = NOW()
                        WHERE user_id = :id AND revogado_em IS NULL
                    """),
                    {"id": user_id},
                )
            conn.commit()
            return result.rowcount > 0

    def alterar_senha_propria(
        self,
        user_id: int,
        senha_atual: str,
        nova_senha: str,
        token_atual: Optional[str] = None,
    ) -> bool:
        token_hash = _hash_session_token(token_atual) if token_atual else None
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT password_hash FROM usuarios WHERE id=:id AND ativo = TRUE"),
                {"id": user_id},
            ).fetchone()
            if not row:
                return False
            try:
                if not _pwd_context_ref.verify(senha_atual, row[0]):
                    return False
            except (ValueError, TypeError):
                logger.warning("Hash de senha invalido para usuario id=%s.", user_id)
                return False

            conn.execute(
                text("UPDATE usuarios SET password_hash=:password_hash WHERE id=:id"),
                {"id": user_id, "password_hash": _pwd_context_ref.hash(nova_senha)},
            )
            conn.execute(
                text("""
                    UPDATE user_sessions
                    SET revogado_em = NOW()
                    WHERE user_id = :id
                      AND revogado_em IS NULL
                      AND (:token_hash IS NULL OR token_hash <> :token_hash)
                """),
                {"id": user_id, "token_hash": token_hash},
            )
            conn.commit()
            return True

    def registrar_auditoria(
        self,
        acao: str,
        ator: Optional[dict] = None,
        entidade: Optional[str] = None,
        entidade_id: Optional[str] = None,
        detalhes: Optional[dict] = None,
        ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        ator = ator or {}
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO auditoria_eventos (
                            ator_user_id, ator_username, acao, entidade, entidade_id,
                            detalhes, ip, user_agent
                        )
                        VALUES (
                            :ator_user_id, :ator_username, :acao, :entidade, :entidade_id,
                            CAST(:detalhes AS JSONB), :ip, :user_agent
                        )
                    """),
                    {
                        "ator_user_id": ator.get("id"),
                        "ator_username": ator.get("username"),
                        "acao": acao,
                        "entidade": entidade,
                        "entidade_id": str(entidade_id) if entidade_id is not None else None,
                        "detalhes": json_dumps(detalhes or {}),
                        "ip": ip,
                        "user_agent": (user_agent or "")[:500] if user_agent else None,
                    },
                )
                conn.commit()
        except Exception as e:
            logger.warning(f"Falha ao registrar auditoria {acao}: {e}")

    def listar_auditoria(self, limit: int = 100) -> pd.DataFrame:
        return self.query(
            """
            SELECT id, ator_user_id, ator_username, acao, entidade, entidade_id,
                   detalhes, ip, criado_em::text AS criado_em
            FROM auditoria_eventos
            ORDER BY criado_em DESC
            LIMIT :limit
            """,
            {"limit": limit},
        )

    def acompanhar_processo(self, user_id: int, processo_id: int) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT numero_cnj FROM processos WHERE id=:id"),
                {"id": processo_id},
            ).fetchone()
            if not row:
                return False
            conn.execute(
                text("""
                    INSERT INTO processos_acompanhados (user_id, processo_id, ativo, atualizado_em)
                    VALUES (:user_id, :processo_id, TRUE, NOW())
                    ON CONFLICT (user_id, processo_id) DO UPDATE
                    SET ativo = TRUE, atualizado_em = NOW()
                """),
                {"user_id": user_id, "processo_id": processo_id},
            )
            conn.execute(
                text("""
                    INSERT INTO alertas_usuario (user_id, processo_id, tipo, titulo, mensagem)
                    VALUES (:user_id, :processo_id, 'processo', 'Processo acompanhado', :mensagem)
                """),
                {
                    "user_id": user_id,
                    "processo_id": processo_id,
                    "mensagem": f"O processo {row[0]} foi adicionado ao monitoramento.",
                },
            )
            conn.commit()
            return True

    def listar_processos_acompanhados(self, user_id: int, limit: int = 100) -> pd.DataFrame:
        return self.query(
            """
            SELECT a.id, a.processo_id, a.criado_em::text AS criado_em,
                   p.numero_cnj, p.classe_processual, p.municipio, p.comarca,
                   p.fase_atual, p.atualizado_em::text AS atualizado_em,
                   s.score_total, s.faixa_probabilidade, s.tipo_pericia_sugerida
            FROM processos_acompanhados a
            JOIN processos p ON p.id = a.processo_id
            LEFT JOIN score_pericial s ON s.processo_id = p.id
            WHERE a.user_id = :user_id AND a.ativo = TRUE
            ORDER BY a.criado_em DESC
            LIMIT :limit
            """,
            {"user_id": user_id, "limit": limit},
        )

    def listar_alertas_usuario(self, user_id: int, limit: int = 100) -> pd.DataFrame:
        return self.query(
            """
            SELECT au.id, au.tipo, au.titulo, au.mensagem, au.lido,
                   au.criado_em::text AS criado_em,
                   p.id AS processo_id, p.numero_cnj, p.classe_processual,
                   p.municipio, p.comarca, p.fase_atual
            FROM alertas_usuario au
            LEFT JOIN processos p ON p.id = au.processo_id
            WHERE au.user_id = :user_id
            ORDER BY au.criado_em DESC
            LIMIT :limit
            """,
            {"user_id": user_id, "limit": limit},
        )

    def marcar_alerta_lido(self, user_id: int, alerta_id: int) -> bool:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    UPDATE alertas_usuario
                    SET lido = TRUE
                    WHERE id = :alerta_id AND user_id = :user_id
                """),
                {"alerta_id": alerta_id, "user_id": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def notify_followers_for_processo(self, processo_id: int, titulo: str, mensagem: str) -> int:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO alertas_usuario (user_id, processo_id, tipo, titulo, mensagem)
                    SELECT user_id, :processo_id, 'processo', :titulo, :mensagem
                    FROM processos_acompanhados
                    WHERE processo_id = :processo_id AND ativo = TRUE
                """),
                {
                    "processo_id": processo_id,
                    "titulo": titulo[:180],
                    "mensagem": (mensagem or "")[:1200],
                },
            )
            conn.commit()
            return max(result.rowcount or 0, 0)

    def iniciar_execucao_coleta(
        self,
        fonte: str,
        tarefa: str,
        parametros: Optional[dict] = None,
    ) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO execucoes_coleta (fonte, tarefa, parametros)
                    VALUES (:fonte, :tarefa, CAST(:parametros AS JSONB))
                    RETURNING id
                """),
                {
                    "fonte": fonte,
                    "tarefa": tarefa,
                    "parametros": json_dumps(parametros or {}),
                },
            ).fetchone()
            conn.commit()
            return int(row[0])

    def finalizar_execucao_coleta(
        self,
        execucao_id: int,
        status: str,
        registros_coletados: int = 0,
        registros_salvos: int = 0,
        erro: Optional[str] = None,
    ) -> None:
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE execucoes_coleta
                    SET status = :status,
                        registros_coletados = :coletados,
                        registros_salvos = :salvos,
                        erro = :erro,
                        finalizado_em = NOW(),
                        duracao_segundos = EXTRACT(EPOCH FROM (NOW() - iniciado_em))
                    WHERE id = :id
                """),
                {
                    "id": execucao_id,
                    "status": status,
                    "coletados": registros_coletados,
                    "salvos": registros_salvos,
                    "erro": (erro or "")[:2000] if erro else None,
                },
            )
            conn.commit()

    def atualizar_execucao_coleta(
        self,
        execucao_id: int,
        registros_coletados: Optional[int] = None,
        registros_salvos: Optional[int] = None,
        erro: Optional[str] = None,
    ) -> None:
        if not execucao_id:
            return
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE execucoes_coleta
                    SET registros_coletados = COALESCE(:coletados, registros_coletados),
                        registros_salvos = COALESCE(:salvos, registros_salvos),
                        erro = COALESCE(:erro, erro)
                    WHERE id = :id
                """),
                {
                    "id": execucao_id,
                    "coletados": registros_coletados,
                    "salvos": registros_salvos,
                    "erro": (erro or "")[:2000] if erro else None,
                },
            )
            conn.commit()

    def tem_coleta_em_execucao(self, fonte: str, max_age_minutes: int = 240) -> bool:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT 1
                    FROM execucoes_coleta
                    WHERE fonte = :fonte
                      AND status = 'running'
                      AND iniciado_em >= NOW() - (:max_age_minutes * INTERVAL '1 minute')
                    LIMIT 1
                """),
                {"fonte": fonte, "max_age_minutes": max(1, int(max_age_minutes))},
            ).fetchone()
            return bool(row)

    def registrar_metrica_coleta_classe(
        self,
        execucao_id: int,
        fonte: str,
        chave: str,
        registros_coletados: int = 0,
        registros_salvos: int = 0,
        descartados_sem_cnj: int = 0,
        duplicados: int = 0,
        status: str = "success",
        erro: Optional[str] = None,
    ) -> None:
        if not execucao_id:
            return
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT INTO metricas_coleta_classe (
                        execucao_id, fonte, chave, status, registros_coletados,
                        registros_salvos, descartados_sem_cnj, duplicados, erro
                    )
                    VALUES (
                        :execucao_id, :fonte, :chave, :status, :coletados,
                        :salvos, :sem_cnj, :duplicados, :erro
                    )
                """),
                {
                    "execucao_id": execucao_id,
                    "fonte": fonte,
                    "chave": chave[:180],
                    "status": status,
                    "coletados": max(0, int(registros_coletados or 0)),
                    "salvos": max(0, int(registros_salvos or 0)),
                    "sem_cnj": max(0, int(descartados_sem_cnj or 0)),
                    "duplicados": max(0, int(duplicados or 0)),
                    "erro": (erro or "")[:2000] if erro else None,
                },
            )
            conn.commit()

    def datajud_data_inicio_incremental(
        self,
        default_start: Optional[str] = None,
        overlap_days: int = 7,
    ) -> Optional[str]:
        if default_start:
            return default_start
        overlap_days = max(0, min(int(overlap_days), 60))
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT (MAX(data_distribuicao) - (:overlap_days * INTERVAL '1 day'))::date::text
                    FROM processos
                    WHERE origem ILIKE '%DataJud%' AND data_distribuicao IS NOT NULL
                """),
                {"overlap_days": overlap_days},
            ).fetchone()
            return row[0] if row and row[0] else None

    def listar_execucoes_coleta(self, limit: int = 50) -> pd.DataFrame:
        return self.query(
            """
            SELECT id, fonte, tarefa, status, parametros,
                   registros_coletados, registros_salvos, erro,
                   iniciado_em::text AS iniciado_em,
                   finalizado_em::text AS finalizado_em,
                   duracao_segundos
            FROM execucoes_coleta
            ORDER BY iniciado_em DESC
            LIMIT :limit
            """,
            {"limit": limit},
        )

    def resumo_execucoes_coleta(self) -> pd.DataFrame:
        return self.query(
            """
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY fonte ORDER BY iniciado_em DESC) AS rn
                FROM execucoes_coleta
            ),
            agg AS (
                SELECT fonte,
                       COUNT(*) AS total_execucoes,
                       SUM(COALESCE(registros_coletados, 0)) AS total_coletados,
                       SUM(COALESCE(registros_salvos, 0)) AS total_salvos,
                       AVG(duracao_segundos) AS duracao_media_segundos,
                       BOOL_OR(status = 'running') AS em_execucao,
                       MAX(iniciado_em) FILTER (WHERE status = 'failed') AS ultima_falha
                FROM execucoes_coleta
                GROUP BY fonte
            )
            SELECT a.fonte,
                   r.status AS ultimo_status,
                   r.tarefa AS ultima_tarefa,
                   r.iniciado_em::text AS ultima_execucao,
                   r.finalizado_em::text AS ultimo_fim,
                   COALESCE(r.registros_coletados, 0) AS ultimos_coletados,
                   COALESCE(r.registros_salvos, 0) AS registros_salvos,
                   r.erro,
                   a.em_execucao,
                   a.total_execucoes,
                   a.total_coletados,
                   a.total_salvos,
                   a.duracao_media_segundos,
                   a.ultima_falha::text AS ultima_falha,
                   CASE
                       WHEN COALESCE(r.erro, '') ILIKE '%429%' OR COALESCE(r.erro, '') ILIKE '%too many requests%'
                           THEN 'Limite de taxa da fonte externa. Aguarde antes de tentar novamente.'
                       WHEN COALESCE(r.erro, '') ILIKE '%401%' OR COALESCE(r.erro, '') ILIKE '%apikey%' OR COALESCE(r.erro, '') ILIKE '%unauthorized%'
                           THEN 'Chave de API ausente ou inválida.'
                       WHEN COALESCE(r.erro, '') ILIKE '%timeout%' OR COALESCE(r.erro, '') ILIKE '%timed out%'
                           THEN 'A fonte externa demorou para responder.'
                       WHEN r.status = 'running'
                           THEN 'Coleta em andamento.'
                       WHEN r.status = 'success' AND COALESCE(r.registros_salvos, 0) = 0
                           THEN 'Coleta concluída sem novos registros.'
                       ELSE ''
                   END AS mensagem_operacional
            FROM agg a
            JOIN ranked r ON r.fonte = a.fonte AND r.rn = 1
            ORDER BY r.iniciado_em DESC
            """
        )

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
        params = {"regiao": regiao} if regiao else {}
        mun_f = (
            "WHERE municipio IN (SELECT nome FROM municipios_mt WHERE regiao_imea = :regiao)"
            if regiao else ""
        )
        desap_f = (
            "WHERE desapropriacao_flag=TRUE AND municipio IN (SELECT nome FROM municipios_mt WHERE regiao_imea = :regiao)"
            if regiao else ""
        )
        proc_f = "WHERE regiao_imea = :regiao" if regiao else ""
        if regiao:
            score_from = (
                "FROM score_pericial s JOIN processos p ON s.processo_id = p.id "
                "WHERE p.regiao_imea = :regiao AND s.faixa_probabilidade = :faixa"
            )
        else:
            score_from = "FROM score_pericial s WHERE s.faixa_probabilidade = :faixa"

        qs = {
            "total_parcelas": ("SELECT COUNT(*) FROM parcelas_sigef " + mun_f, params),
            "total_desapropriadas": ("SELECT COUNT(*) FROM parcelas_sigef " + desap_f, params),
            "area_total_ha": ("SELECT COALESCE(SUM(area_ha),0) FROM desapropriacao_ativa " + mun_f, params),
            "total_portarias": ("SELECT COUNT(*) FROM portarias_diario_oficial " + mun_f, params),
            "total_assentamentos": ("SELECT COUNT(*) FROM assentamentos_incra " + mun_f, params),
            "total_alertas_deter": ("SELECT COUNT(*) FROM inpe_deter", {}),
            "total_processos": ("SELECT COUNT(*) FROM processos " + proc_f, params),
            "processos_quentes": ("SELECT COUNT(*) " + score_from, {**params, "faixa": "janela_quente"}),
            "processos_provaveis": ("SELECT COUNT(*) " + score_from, {**params, "faixa": "provavel"}),
            "ultima_coleta": ("SELECT MAX(coletado_em) FROM portarias_diario_oficial", {}),
        }
        result = {}
        for k, (sql, query_params) in qs.items():
            try:
                result[k] = self.query(sql, query_params).iloc[0, 0]
            except Exception:
                result[k] = 0
        return result


def init_db():
    db = Database()
    db._init_schema()
    logger.info("Banco de dados pronto.")
