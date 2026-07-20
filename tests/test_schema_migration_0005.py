from pathlib import Path


def test_judicial_intelligence_schema_is_versioned():
    migration = (
        Path(__file__).resolve().parents[1]
        / "database"
        / "migrations"
        / "0005_judicial_intelligence.sql"
    )

    assert migration.exists()
    sql = migration.read_text(encoding="utf-8")
    for table in (
        "movimentacoes",
        "publicacoes",
        "eventos_administrativos",
        "score_pericial",
        "portarias_diario_oficial",
        "data_lake_raw",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql

    assert "ux_mov_unique_business" in sql
    assert "ux_portarias_unique_business" in sql
