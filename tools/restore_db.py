"""
Restore operacional do banco PostgreSQL/PostGIS do Radar Pericial.

Requisitos:
    - pg_restore e psql disponiveis no PATH.
    - DATABASE_URL ou variaveis PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE.

Exemplo:
    python tools/restore_db.py backups/radar_pericial_x.dump --yes
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
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


def _confirm(database: str) -> None:
    answer = input(f"Restaurar backup sobre o banco '{database}'? Digite RESTORE para confirmar: ")
    if answer.strip() != "RESTORE":
        raise SystemExit("Restore cancelado.")


def run(path: Path, assume_yes: bool = False) -> None:
    if not path.exists():
        raise SystemExit(f"Arquivo nao encontrado: {path}")

    cfg = _load_config()
    if not assume_yes:
        _confirm(cfg.database)

    env = os.environ.copy()
    if cfg.password:
        env["PGPASSWORD"] = cfg.password

    common = [
        "--host",
        cfg.host,
        "--port",
        cfg.port,
        "--username",
        cfg.user,
        "--dbname",
        cfg.database,
    ]
    if path.suffix.lower() == ".sql":
        cmd = ["psql", *common, "--file", str(path)]
    else:
        cmd = [
            "pg_restore",
            *common,
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-privileges",
            str(path),
        ]

    print(f"Iniciando restore de {path} no banco '{cfg.database}'")
    result = subprocess.run(cmd, env=env, text=True, capture_output=True)
    if result.returncode != 0:
        print(result.stderr.strip() or "restore falhou sem mensagem.", file=sys.stderr)
        raise SystemExit(result.returncode)
    print("Restore concluido.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore do banco do Radar Pericial")
    parser.add_argument("backup", help="Arquivo .dump ou .sql")
    parser.add_argument("--yes", action="store_true", help="Nao pedir confirmacao interativa")
    args = parser.parse_args()
    run(Path(args.backup), assume_yes=args.yes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
