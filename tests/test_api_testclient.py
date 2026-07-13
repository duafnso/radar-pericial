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

    def stats(self, regiao=None):
        return {
            "total_processos": 1,
            "processos_quentes": 1,
            "processos_provaveis": 0,
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

    def listar_usuarios(self):
        return pd.DataFrame(
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
        )


@pytest.fixture()
def api_client(monkeypatch):
    import api.main as main_module

    fake_db = FakeDb()
    main_module._LOGIN_FAILURES.clear()
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
