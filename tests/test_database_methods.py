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
    assert "INSERT INTO alertas_usuario" in conn.executed[2][0]
    assert conn.executed[2][1]["processo_id"] == 10
    assert conn.commits == 2


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


def test_tem_coleta_em_execucao_returns_true_for_running_collection():
    conn = FakeConnection(fetch_rows=[(1,)])
    db = make_db(conn)

    assert db.tem_coleta_em_execucao("judicial") is True
    assert "FROM execucoes_coleta" in conn.executed[0][0]
    assert conn.executed[0][1]["fonte"] == "judicial"


def test_registrar_metrica_coleta_classe_inserts_metrics():
    conn = FakeConnection()
    db = make_db(conn)

    db.registrar_metrica_coleta_classe(
        execucao_id=99,
        fonte="judicial",
        chave="desapropriacao",
        registros_coletados=20,
        registros_salvos=18,
        descartados_sem_cnj=1,
        duplicados=1,
    )

    assert "INSERT INTO metricas_coleta_classe" in conn.executed[0][0]
    assert conn.executed[0][1]["execucao_id"] == 99
    assert conn.executed[0][1]["duplicados"] == 1
    assert conn.commits == 1


def test_listar_metricas_coleta_classe_filters_by_fonte(monkeypatch):
    from database.db import Database

    captured = {}
    db = object.__new__(Database)

    def fake_query(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return pd.DataFrame()

    monkeypatch.setattr(db, "query", fake_query)

    db.listar_metricas_coleta_classe(limit=20, fonte="judicial")

    assert "FROM metricas_coleta_classe" in captured["sql"]
    assert "fonte = :fonte" in captured["sql"]
    assert captured["params"] == {"limit": 20, "fonte": "judicial"}


def test_datajud_data_inicio_incremental_prefers_configured_start():
    conn = FakeConnection()
    db = make_db(conn)

    assert db.datajud_data_inicio_incremental("2026-01-01") == "2026-01-01"
    assert conn.executed == []


def test_marcar_alerta_lido_updates_user_alert():
    conn = FakeConnection()
    db = make_db(conn)

    assert db.marcar_alerta_lido(user_id=3, alerta_id=7) is True
    assert "UPDATE alertas_usuario" in conn.executed[0][0]
    assert conn.executed[0][1] == {"alerta_id": 7, "user_id": 3}
    assert conn.commits == 1


def test_cleanup_expired_sessions_deletes_old_expired_or_revoked_sessions():
    conn = FakeConnection()
    db = make_db(conn)

    removed = db.cleanup_expired_sessions(retention_days=14)

    assert removed == 1
    assert "DELETE FROM user_sessions" in conn.executed[0][0]
    assert conn.executed[0][1] == {"retention_days": 14}
    assert conn.commits == 1


def test_cleanup_expired_sessions_clamps_retention_days():
    conn = FakeConnection()
    db = make_db(conn)

    db.cleanup_expired_sessions(retention_days=999)

    assert conn.executed[0][1] == {"retention_days": 365}


def test_cleanup_operational_history_deletes_old_records():
    conn = FakeConnection()
    db = make_db(conn)

    removed = db.cleanup_operational_history(
        audit_retention_days=365,
        collection_retention_days=180,
        metrics_retention_days=90,
    )

    assert "DELETE FROM auditoria_eventos" in conn.executed[0][0]
    assert "DELETE FROM metricas_coleta_classe" in conn.executed[1][0]
    assert "DELETE FROM execucoes_coleta" in conn.executed[2][0]
    assert removed == {
        "auditoria_eventos": 1,
        "metricas_coleta_classe": 1,
        "execucoes_coleta": 1,
    }
    assert conn.commits == 1


def test_init_schema_does_not_reset_admin_password_by_default(monkeypatch):
    from database.db import Database

    conn = FakeConnection()
    db = make_db(conn)
    monkeypatch.setenv("DEFAULT_ADMIN_PASSWORD", "Admin12345!")
    monkeypatch.delenv("RESET_DEFAULT_ADMIN_PASSWORD", raising=False)

    db._init_schema()

    admin_sql = conn.executed[2][0]
    assert "ON CONFLICT (username) DO NOTHING" in admin_sql
    assert "DO UPDATE" not in admin_sql


def test_auditoria_qualidade_processos_calculates_summary(monkeypatch):
    from database.db import Database

    db = object.__new__(Database)
    values = iter([10, 1, 2, 0, 3, 1, 1, 0, 4])
    captured_sql = []

    def fake_query(sql, params=None):
        captured_sql.append(sql)
        return pd.DataFrame([[next(values)]])

    monkeypatch.setattr(db, "query", fake_query)

    result = db.auditoria_qualidade_processos()

    assert result["total_processos"] == 10
    assert result["score_qualidade"] == 73
    assert result["total_problemas"] == 12
    assert result["problemas"][0]["codigo"] == "sem_cnj"
    assert result["problemas"][4]["codigo"] == "sem_score"
    assert any("municipios_mt" in sql for sql in captured_sql)

def test_resumo_mapa_processos_aggregates_before_city_limit(monkeypatch):
    from database.db import Database

    db = Database.__new__(Database)
    responses = iter([
        pd.DataFrame([{
            "municipio": "Cuiaba",
            "regiao_imea": "Centro-Sul",
            "lat": -15.4156,
            "lng": -56.0517,
            "total_processos": 152,
            "maior_score": 88,
            "processos_quentes": 4,
            "processos_provaveis": 12,
            "faixa_dominante": "janela_quente",
            "ultima_distribuicao": "2026-07-01",
        }]),
        pd.DataFrame([{
            "total_processos": 702,
            "total_municipios": 73,
            "sem_localizacao": 27,
        }]),
    ])
    calls = []

    def fake_query(sql, params=None):
        calls.append((sql, params or {}))
        return next(responses)

    monkeypatch.setattr(db, "query", fake_query)

    result = db.resumo_mapa_processos(
        {
            "regiao": "Centro-Sul",
            "municipio": "cuiaba",
            "faixa": "janela_quente",
            "data_inicio": "2026-01-01",
            "data_fim": "2026-07-19",
        },
        limit_cidades=100,
    )

    assert result["total_processos"] == 702
    assert result["sem_localizacao"] == 27
    assert result["total_municipios"] == 73
    assert result["items"][0]["total_processos"] == 152
    items_sql, items_params = calls[0]
    totals_sql, totals_params = calls[1]
    for sql in (items_sql, totals_sql):
        assert sql.count("LEFT JOIN LATERAL") == 1
        assert "ORDER BY sp.calculado_em DESC NULLS LAST, sp.id DESC" in sql
        assert sql.count("LIMIT 1") == 1
        assert "LEFT JOIN score_pericial s ON s.processo_id = p.id" not in sql
        for clause in (
            "p.regiao_imea = :regiao",
            "p.municipio ILIKE :municipio",
            "s.faixa_probabilidade = :faixa",
            "p.data_distribuicao >= :data_inicio",
            "p.data_distribuicao <= :data_fim",
        ):
            assert clause in sql
    assert "m.nome AS municipio" in items_sql
    assert "COUNT(DISTINCT m.nome)" in totals_sql
    assert "CASE COALESCE(faixa_probabilidade, 'frio')" in items_sql
    assert "COALESCE(faixa_probabilidade, 'frio') ASC" in items_sql
    assert "GROUP BY municipio, geometry" in items_sql
    assert "ST_PointOnSurface(geometry)" in items_sql
    for predicate in ("geometry IS NOT NULL", "ST_IsValid(geometry)", "NOT ST_IsEmpty(geometry)"):
        assert predicate in items_sql
    for predicate in ("m.geometry IS NOT NULL", "ST_IsValid(m.geometry)", "NOT ST_IsEmpty(m.geometry)"):
        assert predicate in totals_sql
    assert items_params == {
        "limit_cidades": 100,
        "regiao": "Centro-Sul",
        "municipio": "%cuiaba%",
        "faixa": "janela_quente",
        "data_inicio": "2026-01-01",
        "data_fim": "2026-07-19",
    }
    assert totals_params == {
        key: value
        for key, value in items_params.items()
        if key != "limit_cidades"
    }


def test_listar_processos_uses_normalized_exact_municipality_without_wildcards(monkeypatch):
    from database.db import Database

    db = Database.__new__(Database)
    calls = []

    def fake_query(sql, params=None):
        calls.append((sql, params or {}))
        if "SELECT COUNT(*)" in sql:
            return pd.DataFrame([[1]])
        return pd.DataFrame([{"id": 1, "municipio": "Vera"}])

    monkeypatch.setattr(db, "query", fake_query)

    result = db.listar_processos({"municipio_exato": "Vera"}, limit=10, offset=0)

    assert result["total"] == 1
    for sql, params in calls:
        assert "lower(regexp_replace(btrim(p.municipio)" in sql
        assert "ILIKE :municipio" not in sql
        assert params["municipio_exato"] == "Vera"
        assert "%" not in params["municipio_exato"]


def test_listar_processos_keeps_partial_municipality_search(monkeypatch):
    from database.db import Database

    db = Database.__new__(Database)
    calls = []

    def fake_query(sql, params=None):
        calls.append((sql, params or {}))
        if "SELECT COUNT(*)" in sql:
            return pd.DataFrame([[1]])
        return pd.DataFrame([{"id": 1, "municipio": "Primavera do Leste"}])

    monkeypatch.setattr(db, "query", fake_query)

    db.listar_processos({"municipio": "Vera"}, limit=10, offset=0)

    for sql, params in calls:
        assert "p.municipio ILIKE :municipio" in sql
        assert params["municipio"] == "%Vera%"
        assert "municipio_exato" not in params
