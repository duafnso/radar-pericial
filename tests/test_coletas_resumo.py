import pandas as pd
import pytest


def test_database_coletas_resumo_query_contains_operational_messages(monkeypatch):
    from database.db import Database

    captured = {}
    db = object.__new__(Database)

    def fake_query(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return pd.DataFrame(
            [
                {
                    "fonte": "judicial",
                    "ultimo_status": "failed",
                    "mensagem_operacional": "Limite de taxa da fonte externa.",
                }
            ]
        )

    monkeypatch.setattr(db, "query", fake_query)

    result = db.resumo_execucoes_coleta()

    sql = captured["sql"].lower()
    assert len(result) == 1
    assert "row_number() over (partition by fonte" in sql
    assert "bool_or(status = 'running')" in sql
    assert "429" in sql
    assert "apikey" in sql
    assert "timeout" in sql


@pytest.mark.asyncio
async def test_coletas_resumo_endpoint_returns_fake_db_rows(monkeypatch):
    import api.main as main_module

    class FakeDb:
        def resumo_execucoes_coleta(self):
            return pd.DataFrame(
                [
                    {
                        "fonte": "judicial",
                        "ultimo_status": "success",
                        "ultima_execucao": "2026-07-03T10:00:00",
                        "registros_salvos": 12,
                        "erro": None,
                        "em_execucao": False,
                    }
                ]
            )

    monkeypatch.setattr(main_module, "_db", FakeDb())

    response = await main_module.coletas_resumo(
        _user={"id": 1, "username": "admin", "role": "admin"}
    )

    assert response["total"] == 1
    assert response["items"][0]["fonte"] == "judicial"
    assert response["items"][0]["ultimo_status"] == "success"
    assert response["items"][0]["registros_salvos"] == 12
