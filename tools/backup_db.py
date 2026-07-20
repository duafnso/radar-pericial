"""
Backup operacional do banco PostgreSQL/PostGIS do Radar Pericial.

Requisitos:
    - pg_dump disponivel no PATH.
    - DATABASE_URL ou variaveis PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE.

Exemplo:
    python tools/backup_db.py --output-dir backups
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: str
    user: str
    password: str
    database: str


def _load_config() -> PgConfig:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        parsed = urlparse(database_url)
        scheme = parsed.scheme.replace("+psycopg2", "")
        if scheme not in {"postgres", "postgresql"}:
            raise ValueError("DATABASE_URL deve usar postgres/postgresql.")
        return PgConfig(
            host=parsed.hostname or "localhost",
            port=str(parsed.port or 5432),
            user=unquote(parsed.username or "postgres"),
            password=unquote(parsed.password or ""),
            database=(parsed.path or "/postgres").lstrip("/"),
        )

    return PgConfig(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        database=os.getenv("PGDATABASE", "radar_pericial"),
    )


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run(output_dir: Path, plain_sql: bool = False) -> Path:
    cfg = _load_config()
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = "sql" if plain_sql else "dump"
    output = output_dir / f"radar_pericial_{cfg.database}_{_timestamp()}.{suffix}"

    cmd = [
        "pg_dump",
        "--host",
        cfg.host,
        "--port",
        cfg.port,
        "--username",
        cfg.user,
        "--dbname",
        cfg.database,
        "--file",
        str(output),
    ]
    if not plain_sql:
        cmd.extend(["--format", "custom"])

    env = os.environ.copy()
    if cfg.password:
        env["PGPASSWORD"] = cfg.password

    print(f"Iniciando backup do banco '{cfg.database}' em {output}")
    result = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if result.returncode != 0:
        if output.exists():
            output.unlink()
        print(result.stderr.strip() or "pg_dump falhou sem mensagem.", file=sys.stderr)
        raise SystemExit(result.returncode)
    print(f"Backup concluido: {output}")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Backup do banco do Radar Pericial")
    parser.add_argument("--output-dir", default="backups", help="Diretorio de destino")
    parser.add_argument("--plain-sql", action="store_true", help="Gerar SQL texto em vez de formato custom")
    args = parser.parse_args()
    run(Path(args.output_dir), plain_sql=args.plain_sql)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
