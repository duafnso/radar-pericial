import pandas as pd


class FakeResult:
    def __init__(self, row=None, rowcount=1):
        self._row = row
        self.rowcount = rowcount

    def fetchone(self):
        return self._row


class FakeConnection:
    def __init__(self, fetch_rows=None):
        self.fetch_rows = list(fetch_rows or [])
        self.executed = []
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None

    def execute(self, sql, params=None):
        sql_text = str(sql)
        self.executed.append((sql_text, params))
        if self.fetch_rows:
            return FakeResult(row=self.fetch_rows.pop(0))
        if "RETURNING id" in sql_text:
            return FakeResult(row=(123,))
        return FakeResult(row=None, rowcount=1)

    def commit(self):
        self.commits += 1


class FakeEngine:
    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return self.connection


def make_db(connection):
    from database.db import Database

    db = object.__new__(Database)
    db.engine = FakeEngine(connection)
    return db


def test_upsert_processo_inserts_when_cnj_is_new():
    conn = FakeConnection(fetch_rows=[None, (123,)])
    db = make_db(conn)

    processo_id = db.upsert_processo(
        {
            "numero_cnj": "0000001-00.2026.8.11.0001",
            "tribunal": "TJMT",
            "classe_processual": "Desapropriacao",
        }
    )

    assert processo_id == 123
    assert "SELECT id FROM processos" in conn.executed[0][0]
    assert "INSERT INTO processos" in conn.executed[1][0]
    assert conn.commits == 1


def test_upsert_processo_updates_when_cnj_exists():
    conn = FakeConnection(fetch_rows=[(77,)])
    db = make_db(conn)

    processo_id = db.upsert_processo(
        {
            "numero_cnj": "0000001-00.2026.8.11.0001",
            "tribunal": "TJMT",
            "municipio": "Cuiaba",
        }
    )

    assert processo_id == 77
    assert "UPDATE processos" in conn.executed[1][0]
    assert conn.executed[1][1]["id"] == 77
    assert conn.commits == 1


def test_save_score_replaces_existing_score():
    conn = FakeConnection()
    db = make_db(conn)

    db.save_score(
        10,
        {
            "score_total": 90,
            "faixa_probabilidade": "janela_quente",
            "faixa_label": "Janela quente",
            "urgencia": "alta",
        },
    )

    assert "DELETE FROM score_pericial" in conn.executed[0][0]
    assert "INSERT INTO score_pericial" in conn.executed[1][0]
    assert conn.executed[1][1]["processo_id"] == 10
    assert conn.executed[1][1]["score_total"] == 90
    assert conn.commits == 1


def test_save_movimentacao_ignores_existing_duplicate():
    conn = FakeConnection(fetch_rows=[(55,)])
    db = make_db(conn)

    db.save_movimentacao(
        10,
        {
            "data_movimentacao": "2026-07-03",
            "descricao": "Nomeacao de perito",
            "fonte": "DataJud",
            "score_evento": 30,
        },
    )

    assert len(conn.executed) == 1
    assert "SELECT id FROM movimentacoes" in conn.executed[0][0]
    assert conn.commits == 0


def test_save_movimentacao_inserts_new_row():
    conn = FakeConnection(fetch_rows=[None])
    db = make_db(conn)

    db.save_movimentacao(
        10,
        {
            "data_movimentacao": "2026-07-03",
            "descricao": "Nomeacao de perito",
            "fonte": "DataJud",
            "score_evento": 30,
        },
    )

    assert "INSERT INTO movimentacoes" in conn.executed[1][0]
    assert conn.executed[1][1]["pid"] == 10
    assert conn.commits == 1


def test_save_portarias_deduplicates_existing_rows(monkeypatch):
    conn = FakeConnection()
    db = make_db(conn)
    existing = pd.DataFrame(
        [
            {
                "titulo": "Portaria duplicada",
                "data_publicacao": "2026-07-03",
                "fonte": "DOU",
            }
        ]
    )
    monkeypatch.setattr(db, "query", lambda *_args, **_kwargs: existing)

    db.save_portarias(
        [
            {
                "titulo": "Portaria duplicada",
                "data_publicacao": "2026-07-03",
                "fonte": "DOU",
            },
            {
                "titulo": "Portaria nova",
                "data_publicacao": "2026-07-03",
                "fonte": "DOU",
                "score_evento": 40,
            },
        ]
    )

    assert len(conn.executed) == 1
    assert "INSERT INTO portarias_diario_oficial" in conn.executed[0][0]
    assert len(conn.executed[0][1]) == 1
    assert conn.executed[0][1][0]["titulo"] == "Portaria nova"


def test_atualizar_execucao_coleta_updates_partial_progress():
    conn = FakeConnection()
    db = make_db(conn)

    db.atualizar_execucao_coleta(
        execucao_id=99,
        registros_coletados=20,
        registros_salvos=18,
        erro="parcial",
    )

    assert "UPDATE execucoes_coleta" in conn.executed[0][0]
    assert conn.executed[0][1] == {
        "id": 99,
        "coletados": 20,
        "salvos": 18,
        "erro": "parcial",
    }
    assert conn.commits == 1


def test_atualizar_execucao_coleta_ignores_empty_id():
    conn = FakeConnection()
    db = make_db(conn)

    db.atualizar_execucao_coleta(execucao_id=0, registros_coletados=20)

    assert conn.executed == []
    assert conn.commits == 0
