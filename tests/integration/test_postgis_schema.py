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


def test_postgis_localizable_geometry_predicate_excludes_invalid_and_empty_shapes():
    if os.getenv("RUN_POSTGIS_INTEGRATION") != "true":
        pytest.skip("PostGIS integration test disabled")

    database_url = os.environ["DATABASE_URL"]
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    engine = create_engine(database_url, pool_pre_ping=True)
    with engine.connect() as conn:
        result = conn.execute(text("""
            WITH geometries(kind, geometry) AS (
                VALUES
                    ('valid', ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))')),
                    ('invalid', ST_GeomFromText('POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))')),
                    ('empty', ST_GeomFromText('POLYGON EMPTY')),
                    ('missing', NULL::geometry)
            ), classified AS (
                SELECT *, geometry IS NOT NULL AND ST_IsValid(geometry) AND NOT ST_IsEmpty(geometry) AS localizable
                FROM geometries
            )
            SELECT COUNT(*) FILTER (WHERE localizable) AS located,
                   COUNT(*) FILTER (WHERE NOT localizable) AS without_location,
                   COUNT(ST_PointOnSurface(geometry)) FILTER (WHERE localizable) AS drawable
            FROM classified
        """)).mappings().one()

    assert result == {"located": 1, "without_location": 3, "drawable": 1}
