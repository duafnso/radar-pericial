import sys
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi.testclient import TestClient


class FakeDb:
    def __init__(self):
        self.audit_events = []
        self.revoked = []
        self.collection_running = False
        self.read_alerts = []
        self.tokens = {
            "token-admin": {"id": 1, "username": "admin", "role": "admin"},
            "token-operator": {"id": 2, "username": "operator", "role": "operator"},
            "token-user": {"id": 3, "username": "user", "role": "user"},
            "token-viewer": {"id": 4, "username": "viewer", "role": "viewer"},
        }

    def check_login(self, username, password):
        return username == "admin" and password == "Admin12345!"

    def create_token(self, username, user_agent=None, client_ip=None):
        return "token-admin"

    def validate_token_user(self, token, user_agent=None, client_ip=None):
        return self.tokens.get(token)

    def revoke_token(self, token):
        self.revoked.append(token)

    def registrar_auditoria(self, **kwargs):
        self.audit_events.append(kwargs)

    def query(self, sql, params=None):
        if "SELECT 1" in sql:
            return pd.DataFrame([[1]])
        if "COUNT(*) FROM processos" in sql:
            return pd.DataFrame([[1]])
        if "FROM processos" in sql:
            return pd.DataFrame(
                [
                    {
                        "id": 10,
                        "numero_cnj": "0000001-00.2026.8.11.0001",
                        "tribunal": "TJMT",
                        "comarca": "Cuiaba",
                        "vara": "Vara Agraria",
                        "classe_processual": "Desapropriacao",
                        "assunto_principal": "Imovel rural",
                        "data_distribuicao": "2026-07-01",
                        "fase_atual": "Instrucao",
                        "municipio": "Cuiaba",
                        "regiao_imea": "Centro-Sul",
                        "origem": "DataJud",
                        "score_total": 88,
                        "faixa_probabilidade": "janela_quente",
                        "faixa_label": "Janela quente",
                        "tipo_pericia_sugerida": "Avaliacao agronomica",
                        "categorias_detectadas": "desapropriacao",
                        "urgencia": "alta",
                    }
                ]
            )
        return pd.DataFrame()

    def resumo_mapa_processos(self, filtros, limit_cidades=200):
        self.map_filters = filtros
        self.map_limit_cidades = limit_cidades
        return {
            "total_processos": 702,
            "total_municipios": 1,
            "sem_localizacao": 27,
            "items": [{
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
            }],
        }
    def stats(self, regiao=None):
        return {
            "total_processos": 1,
            "processos_quentes": 1,
            "processos_provaveis": 0,
        }

    def auditoria_qualidade_processos(self):
        return {
            "total_processos": 1,
            "score_qualidade": 96,
            "total_problemas": 1,
            "problemas": [
                {"codigo": "sem_score", "rotulo": "Processos sem score", "total": 1, "severidade": "alta"}
            ],
            "recomendacoes": ["Reexecutar score quando houver processos sem score."],
        }
    def listar_execucoes_coleta(self, limit=50):
        return pd.DataFrame(
            [
                {
                    "id": 1,
                    "fonte": "judicial",
                    "tarefa": "task_judicial",
                    "status": "success",
                    "registros_coletados": 5,
                    "registros_salvos": 4,
                    "erro": None,
                    "iniciado_em": "2026-07-03T08:00:00",
                    "finalizado_em": "2026-07-03T08:01:00",
                    "duracao_segundos": 60,
                }
            ]
        )

    def tem_coleta_em_execucao(self, fonte, max_age_minutes=240):
        return self.collection_running

    def resumo_execucoes_coleta(self):
        return pd.DataFrame(
            [
                {
                    "fonte": "judicial",
                    "ultimo_status": "success",
                    "ultima_execucao": "2026-07-03T08:00:00",
                    "registros_salvos": 4,
                    "erro": None,
                    "em_execucao": False,
                    "mensagem_operacional": "",
                }
            ]
        )

    def listar_metricas_coleta_classe(self, limit=200, fonte=None, execucao_id=None):
        return pd.DataFrame(
            [
                {
                    "id": 1,
                    "execucao_id": 20,
                    "fonte": fonte or "judicial",
                    "chave": "desapropriacao",
                    "status": "success",
                    "registros_coletados": 12,
                    "registros_salvos": 10,
                    "descartados_sem_cnj": 1,
                    "duplicados": 1,
                    "erro": None,
                    "criado_em": "2026-07-13T08:00:00",
                }
            ]
        )

    def acompanhar_processo(self, user_id, processo_id):
        return processo_id == 10

    def listar_processos_acompanhados(self, user_id, limit=100):
        return pd.DataFrame(
            [
                {
                    "id": 1,
                    "processo_id": 10,
                    "numero_cnj": "0000001-00.2026.8.11.0001",
                    "classe_processual": "Desapropriacao",
                    "municipio": "Cuiaba",
                    "criado_em": "2026-07-03T08:00:00",
                }
            ]
        )

    def listar_alertas_usuario(self, user_id, limit=100):
        return pd.DataFrame(
            [
                {
                    "id": 7,
                    "tipo": "processo",
                    "titulo": "Processo acompanhado",
                    "mensagem": "Houve movimentacao no processo.",
                    "lido": False,
                    "criado_em": "2026-07-03T08:00:00",
                    "processo_id": 10,
                    "numero_cnj": "0000001-00.2026.8.11.0001",
                    "municipio": "Cuiaba",
                }
            ]
        )

    def marcar_alerta_lido(self, user_id, alerta_id):
        if alerta_id == 7:
            self.read_alerts.append((user_id, alerta_id))
            return True
        return False

    def listar_usuarios(self, role=None, ativo=None, busca=None):
        rows = [
            [
                {
                    "id": 1,
                    "username": "admin",
                    "role": "admin",
                    "ativo": True,
                    "regiao_foco": None,
                    "criado_em": "2026-07-01T00:00:00",
                }
            ]
        ][0]
        if role:
            rows = [row for row in rows if row["role"] == role]
        if ativo is not None:
            rows = [row for row in rows if row["ativo"] is ativo]
        if busca:
            rows = [row for row in rows if busca.lower() in row["username"].lower()]
        return pd.DataFrame(rows)

    def listar_auditoria(self, limit=100, acao=None, ator=None, entidade=None, data_inicio=None, data_fim=None):
        return pd.DataFrame(
            [
                {
                    "id": 1,
                    "ator_user_id": 1,
                    "ator_username": "admin",
                    "acao": "login",
                    "entidade": "usuario",
                    "entidade_id": "1",
                    "detalhes": {},
                    "ip": "127.0.0.1",
                    "criado_em": "2026-07-01T00:00:00",
                }
            ]
        )


@pytest.fixture()
def api_client(monkeypatch):
    import api.main as main_module

    fake_db = FakeDb()
    main_module._LOGIN_FAILURES.clear()
    main_module._ACTION_FAILURES.clear()
    monkeypatch.setenv("RATE_LIMIT_BACKEND", "memory")
    monkeypatch.setattr(main_module, "_db", fake_db)
    return TestClient(main_module.app), fake_db


def auth_header(token):
    return {"Authorization": f"Bearer {token}"}


def test_health_endpoint(api_client):
    client, _ = api_client

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_ready_endpoint_with_mocked_dependencies(api_client, monkeypatch):
    client, _ = api_client

    class FakeRedisClient:
        def ping(self):
            return True

        def close(self):
            pass

    class FakeCeleryConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def ensure_connection(self, max_retries=1):
            return None

    fake_redis = SimpleNamespace(from_url=lambda *args, **kwargs: FakeRedisClient())
    fake_celery_app = SimpleNamespace(
        connection_for_read=lambda: FakeCeleryConnection()
    )
    monkeypatch.setitem(sys.modules, "redis", fake_redis)
    monkeypatch.setitem(sys.modules, "alerts.scheduler", SimpleNamespace(app=fake_celery_app))

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["dependencies"] == {
        "database": True,
        "redis": True,
        "celery": True,
    }


def test_login_valid_returns_token_and_user(api_client):
    client, _ = api_client

    response = client.post(
        "/api/login",
        json={"username": "admin", "password": "Admin12345!"},
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["status"] == "ok"
    assert payload["token"] == "token-admin"
    assert payload["user"]["role"] == "admin"


def test_login_invalid_returns_401(api_client):
    client, _ = api_client

    response = client.post(
        "/api/login",
        json={"username": "admin", "password": "wrong-password"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Credenciais incorretas"


def test_api_me_requires_bearer_token(api_client):
    client, _ = api_client

    response = client.get("/api/me")

    assert response.status_code == 401


def test_api_me_returns_current_user(api_client):
    client, _ = api_client

    response = client.get("/api/me", headers=auth_header("token-operator"))

    assert response.status_code == 200
    assert response.json()["user"]["role"] == "operator"


@pytest.mark.parametrize(
    ("token", "expected_status"),
    [
        ("token-admin", 200),
        ("token-operator", 403),
        ("token-user", 403),
        ("token-viewer", 403),
    ],
)
def test_admin_users_permission_by_role(api_client, token, expected_status):
    client, _ = api_client

    response = client.get("/api/admin/usuarios", headers=auth_header(token))

    assert response.status_code == expected_status


def test_admin_users_accepts_filters(api_client):
    client, _ = api_client

    response = client.get(
        "/api/admin/usuarios?role=admin&ativo=true&busca=adm",
        headers=auth_header("token-admin"),
    )

    assert response.status_code == 200
    assert response.json()["total"] == 1


def test_auditoria_accepts_filters_for_admin(api_client):
    client, _ = api_client

    response = client.get(
        "/api/admin/auditoria?acao=login&ator=admin&entidade=usuario&limit=50",
        headers=auth_header("token-admin"),
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["acao"] == "login"


def test_processos_endpoint_returns_items(api_client):
    client, _ = api_client

    response = client.get(
        "/api/processos?limit=5&faixa=janela_quente",
        headers=auth_header("token-viewer"),
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["total"] == 1
    assert payload["items"][0]["numero_cnj"] == "0000001-00.2026.8.11.0001"
    assert payload["items"][0]["score_total"] == 88


def test_coletas_status_requires_operational_permission(api_client):
    client, _ = api_client

    response = client.get(
        "/api/coletas/status?limit=5",
        headers=auth_header("token-user"),
    )

    assert response.status_code == 403


def test_coletas_status_returns_history_for_operator(api_client):
    client, _ = api_client

    response = client.get(
        "/api/coletas/status?limit=5",
        headers=auth_header("token-operator"),
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["total"] == 1
    assert payload["items"][0]["fonte"] == "judicial"
    assert payload["items"][0]["registros_salvos"] == 4


def test_coletas_metricas_returns_diagnostics_for_operator(api_client):
    client, _ = api_client

    response = client.get(
        "/api/coletas/metricas?fonte=judicial&limit=5",
        headers=auth_header("token-operator"),
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["total"] == 1
    assert payload["items"][0]["chave"] == "desapropriacao"
    assert payload["items"][0]["duplicados"] == 1


def test_qualidade_processos_requires_operational_permission(api_client):
    client, _ = api_client

    response = client.get(
        "/api/qualidade/processos",
        headers=auth_header("token-user"),
    )

    assert response.status_code == 403



def test_qualidade_processos_returns_audit_for_operator(api_client):
    client, _ = api_client

    response = client.get(
        "/api/qualidade/processos",
        headers=auth_header("token-operator"),
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["score_qualidade"] == 96
    assert payload["problemas"][0]["codigo"] == "sem_score"

def test_manual_collection_enqueue_is_mocked(api_client, monkeypatch):
    client, fake_db = api_client

    delayed = []

    class FakeTask:
        def __init__(self, name):
            self.name = name

        def delay(self, **kwargs):
            delayed.append((self.name, kwargs))
            return SimpleNamespace(id=f"{self.name}-id")

    monkeypatch.setitem(
        sys.modules,
        "alerts.scheduler",
        SimpleNamespace(
            task_admin=FakeTask("admin"),
            task_geo=FakeTask("geo"),
            task_judicial=FakeTask("judicial"),
            task_score=FakeTask("score"),
        ),
    )

    response = client.post(
        "/api/coletas/judicial/executar",
        headers=auth_header("token-operator"),
        json={},
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload == {
        "status": "queued",
        "tipo": "judicial",
        "task_id": "judicial-id",
    }
    assert delayed == [("judicial", {"dias_atras": 1})]
    assert fake_db.audit_events[-1]["acao"] == "coleta_manual_enfileirada"


def test_manual_collection_rejects_duplicate_running_collection(api_client, monkeypatch):
    client, fake_db = api_client
    fake_db.collection_running = True

    monkeypatch.setitem(
        sys.modules,
        "alerts.scheduler",
        SimpleNamespace(
            task_admin=SimpleNamespace(delay=lambda **kwargs: None),
            task_geo=SimpleNamespace(delay=lambda **kwargs: None),
            task_judicial=SimpleNamespace(delay=lambda **kwargs: None),
            task_score=SimpleNamespace(delay=lambda **kwargs: None),
        ),
    )

    response = client.post(
        "/api/coletas/judicial/executar",
        headers=auth_header("token-operator"),
        json={},
    )

    assert response.status_code == 409
    assert "coleta deste tipo" in response.json()["detail"]


def test_follow_process_and_alerts_flow(api_client):
    client, fake_db = api_client

    followed = client.post(
        "/api/processos/10/acompanhar",
        headers=auth_header("token-user"),
    )
    alerts = client.get("/api/alertas?limit=10", headers=auth_header("token-user"))
    read = client.patch("/api/alertas/7/lido", headers=auth_header("token-user"), json={})

    assert followed.status_code == 200
    assert alerts.status_code == 200
    assert alerts.json()["items"][0]["origem_alerta"] == "processo_acompanhado"
    assert read.status_code == 200
    assert fake_db.read_alerts == [(3, 7)]


def test_sensitive_action_rate_limit_blocks_collection(api_client, monkeypatch):
    client, _ = api_client
    monkeypatch.setenv("SENSITIVE_ACTION_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("SENSITIVE_ACTION_WINDOW_SECONDS", "300")

    delayed = []

    class FakeTask:
        def delay(self, **kwargs):
            delayed.append(kwargs)
            return SimpleNamespace(id="task-id")

    monkeypatch.setitem(
        sys.modules,
        "alerts.scheduler",
        SimpleNamespace(
            task_admin=FakeTask(),
            task_geo=FakeTask(),
            task_judicial=FakeTask(),
            task_score=FakeTask(),
        ),
    )

    first = client.post("/api/coletas/score/executar", headers=auth_header("token-operator"), json={})
    second = client.post("/api/coletas/score/executar", headers=auth_header("token-operator"), json={})

    assert first.status_code == 200
    assert second.status_code == 429




def test_processos_mapa_resumo_returns_aggregated_cities(api_client):
    client, fake_db = api_client

    response = client.get(
        "/api/processos/mapa/resumo?regiao=Centro-Sul&faixa=janela_quente"
        "&municipio=Cuiaba&data_inicio=2026-01-01&data_fim=2026-07-19"
        "&limit_cidades=100",
        headers=auth_header("token-viewer"),
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["total_processos"] == 702
    assert payload["total_municipios"] == 1
    assert payload["items"][0]["municipio"] == "Cuiaba"
    assert fake_db.map_filters["regiao"] == "Centro-Sul"
    assert fake_db.map_filters["faixa"] == "janela_quente"
    assert fake_db.map_filters["municipio"] == "Cuiaba"
    assert fake_db.map_filters["data_inicio"] == "2026-01-01"
    assert fake_db.map_filters["data_fim"] == "2026-07-19"
    assert fake_db.map_limit_cidades == 100


def test_processos_mapa_resumo_requires_authentication(api_client):
    client, _ = api_client

    response = client.get("/api/processos/mapa/resumo")

    assert response.status_code == 401


@pytest.mark.parametrize("limit_cidades", [0, 201])
def test_processos_mapa_resumo_rejects_invalid_city_limit(
    api_client,
    limit_cidades,
):
    client, _ = api_client

    response = client.get(
        f"/api/processos/mapa/resumo?limit_cidades={limit_cidades}",
        headers=auth_header("token-viewer"),
    )

    assert response.status_code == 422


def test_processos_mapa_resumo_returns_503_when_database_is_unavailable(
    api_client,
    monkeypatch,
):
    import api.main as main_module

    client, _ = api_client
    monkeypatch.setattr(main_module, "_db", None)

    response = client.get(
        "/api/processos/mapa/resumo",
        headers=auth_header("token-viewer"),
    )

    assert response.status_code == 503
    assert response.json()["detail"] == "Banco de dados n\u00e3o inicializado"


def test_processos_mapa_resumo_returns_500_on_unexpected_error(
    api_client,
    monkeypatch,
):
    client, fake_db = api_client

    def raise_unexpected_error(*_args, **_kwargs):
        raise RuntimeError("database failed")

    monkeypatch.setattr(
        fake_db,
        "resumo_mapa_processos",
        raise_unexpected_error,
    )

    response = client.get(
        "/api/processos/mapa/resumo",
        headers=auth_header("token-viewer"),
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Erro ao resumir mapa de processos"
