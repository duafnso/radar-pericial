from pathlib import Path


def test_score_explanation_column_is_versioned():
    migration = (
        Path(__file__).resolve().parents[1]
        / "database"
        / "migrations"
        / "0006_score_explanation.sql"
    )

    assert migration.exists()
    sql = migration.read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS explicacao_score TEXT" in sql
