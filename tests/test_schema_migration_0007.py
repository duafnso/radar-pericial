from pathlib import Path


def test_municipality_normalization_backfill_is_versioned():
    migration = (
        Path(__file__).resolve().parents[1]
        / "database"
        / "migrations"
        / "0007_normalize_process_municipalities.sql"
    )

    assert migration.exists()
    sql = migration.read_text(encoding="utf-8")
    assert "Mirassol d''Oeste" in sql
    assert "Lucas do Rio Verde" in sql
