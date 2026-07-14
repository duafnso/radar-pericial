import hashlib
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = BASE_DIR / "database" / "migrations"


def database_url() -> str:
    raw = os.getenv("DATABASE_URL")
    if not raw:
        host = os.getenv("PGHOST", "localhost")
        port = os.getenv("PGPORT", "5432")
        user = os.getenv("PGUSER", "postgres")
        password = quote_plus(os.getenv("PGPASSWORD", ""))
        database = os.getenv("PGDATABASE", "radar_pericial")
        raw = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+psycopg2://", 1)
    return raw


def migration_files() -> list[Path]:
    if not MIGRATIONS_DIR.exists():
        raise RuntimeError(f"Diretorio de migracoes nao encontrado: {MIGRATIONS_DIR}")
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def checksum_sql(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def ensure_table(conn) -> None:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))


def applied_migrations(conn) -> dict[str, str]:
    rows = conn.execute(text("SELECT version, checksum FROM schema_migrations")).fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def apply_all(dry_run: bool = False) -> list[str]:
    engine = create_engine(database_url(), pool_pre_ping=True)
    applied_versions: list[str] = []
    with engine.begin() as conn:
        ensure_table(conn)
        applied = applied_migrations(conn)
        for path in migration_files():
            version = path.stem
            sql = path.read_text(encoding="utf-8-sig")
            checksum = checksum_sql(sql)
            if version in applied:
                if applied[version] != checksum:
                    raise RuntimeError(
                        f"Checksum divergente para migracao ja aplicada: {version}"
                    )
                logger.info("Migracao ja aplicada: %s", version)
                continue
            logger.info("%s migracao: %s", "Validando" if dry_run else "Aplicando", version)
            if dry_run:
                continue
            conn.execute(text(sql))
            conn.execute(
                text("""
                    INSERT INTO schema_migrations (version, checksum)
                    VALUES (:version, :checksum)
                """),
                {"version": version, "checksum": checksum},
            )
            applied_versions.append(version)
    return applied_versions


def main() -> None:
    dry_run = os.getenv("MIGRATIONS_DRY_RUN", "").strip().lower() in {"1", "true", "yes"}
    applied = apply_all(dry_run=dry_run)
    if dry_run:
        logger.info("Dry-run concluido.")
    elif applied:
        logger.info("Migracoes aplicadas: %s", ", ".join(applied))
    else:
        logger.info("Nenhuma migracao pendente.")


if __name__ == "__main__":
    main()

