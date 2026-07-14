import os

import pytest
from sqlalchemy import create_engine, text


pytestmark = pytest.mark.integration


def test_postgis_schema_and_migrations_are_available():
    if os.getenv("RUN_POSTGIS_INTEGRATION") != "true":
        pytest.skip("PostGIS integration test disabled")

    database_url = os.environ["DATABASE_URL"]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.connect() as conn:
        postgis = conn.execute(text("SELECT postgis_version()")).scalar()
        migrations = conn.execute(text("SELECT COUNT(*) FROM schema_migrations")).scalar()
        metrics_table = conn.execute(text("""
            SELECT to_regclass('public.metricas_coleta_classe') IS NOT NULL
        """)).scalar()

    assert postgis
    assert migrations >= 1
    assert metrics_table is True
