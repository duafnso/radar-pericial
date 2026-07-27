import importlib
from types import SimpleNamespace

import pytest


def test_database_requires_session_secret_in_production(monkeypatch):
    import database.db as db_module

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("SESSION_TOKEN_PEPPER", raising=False)
    monkeypatch.delenv("SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError, match="SESSION_TOKEN_PEPPER"):
        db_module._load_session_token_pepper()


def test_database_accepts_secret_key_as_session_pepper(monkeypatch):
    import database.db as db_module

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("SESSION_TOKEN_PEPPER", raising=False)
    monkeypatch.setenv("SECRET_KEY", "test-secret")

    assert db_module._load_session_token_pepper() == "test-secret"


def test_api_requires_cors_origins_in_production(monkeypatch):
    import api.main as main_module

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)

    with pytest.raises(RuntimeError, match="CORS_ALLOW_ORIGINS"):
        main_module._parse_cors_origins()


def test_api_rejects_wildcard_cors_in_production(monkeypatch):
    import api.main as main_module

    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "*")

    with pytest.raises(RuntimeError, match="pode conter"):
        main_module._parse_cors_origins()


def test_api_allows_local_cors_default_in_development(monkeypatch):
    import api.main as main_module

    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)

    assert main_module._parse_cors_origins() == [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ]


def test_api_docs_disabled_by_default_in_production(monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("SESSION_TOKEN_PEPPER", "test-pepper")
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "https://radar.example")
    monkeypatch.delenv("ENABLE_API_DOCS", raising=False)

    import api.main as main_module

    reloaded = importlib.reload(main_module)

    assert reloaded.app.docs_url is None
    assert reloaded.app.redoc_url is None
    assert reloaded.app.openapi_url is None


def test_login_throttle_blocks_after_configured_failures(monkeypatch):
    import api.main as main_module

    main_module._LOGIN_FAILURES.clear()
    monkeypatch.setenv("LOGIN_MAX_ATTEMPTS", "2")
    monkeypatch.setenv("LOGIN_WINDOW_SECONDS", "300")
    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))

    key = main_module._assert_login_allowed(request, "admin")
    main_module._record_login_failure(key)
    key = main_module._assert_login_allowed(request, "admin")
    main_module._record_login_failure(key)

    with pytest.raises(main_module.HTTPException) as exc:
        main_module._assert_login_allowed(request, "admin")

    assert exc.value.status_code == 429
    assert exc.value.headers == {"Retry-After": "300"}


def test_login_throttle_clears_failures(monkeypatch):
    import api.main as main_module

    main_module._LOGIN_FAILURES.clear()
    monkeypatch.setenv("LOGIN_MAX_ATTEMPTS", "1")
    monkeypatch.setenv("LOGIN_WINDOW_SECONDS", "300")
    request = SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"))

    key = main_module._assert_login_allowed(request, "admin")
    main_module._record_login_failure(key)
    main_module._clear_login_failures(key)

    assert main_module._assert_login_allowed(request, "admin") == key


def test_admin_dependency_accepts_admin_role():
    import api.main as main_module

    user = {"id": 1, "username": "admin", "role": "admin"}

    assert main_module.get_current_admin(user) == user


def test_admin_dependency_rejects_non_admin_role():
    import api.main as main_module

    with pytest.raises(main_module.HTTPException) as exc:
        main_module.get_current_admin({"id": 2, "username": "user", "role": "user"})

    assert exc.value.status_code == 403


def test_permission_matrix_allows_operator_to_run_collections():
    import api.main as main_module

    user = {"id": 2, "username": "operator", "role": "operator"}

    assert main_module.user_has_permission(user, "run_collections") is True
    assert main_module.user_has_permission(user, "manage_users") is False


def test_permission_matrix_keeps_viewer_read_only():
    import api.main as main_module

    viewer = {"id": 3, "username": "viewer", "role": "viewer"}

    assert main_module.user_has_permission(viewer, "read_data") is True
    assert main_module.user_has_permission(viewer, "calculate_score") is False
    assert main_module.user_has_permission(viewer, "run_collections") is False


def test_require_permission_accepts_authorized_role():
    import api.main as main_module

    dependency = main_module.require_permission("create_perito")
    operator = {"id": 2, "username": "operator", "role": "operator"}

    assert dependency(operator) == operator


def test_require_permission_rejects_unauthorized_role():
    import api.main as main_module

    dependency = main_module.require_permission("view_audit")

    with pytest.raises(main_module.HTTPException) as exc:
        dependency({"id": 2, "username": "operator", "role": "operator"})

    assert exc.value.status_code == 403


def test_validate_role_uses_declared_roles():
    import api.main as main_module

    for role in main_module.ALLOWED_ROLES:
        assert main_module._validate_role(role) == role

    with pytest.raises(main_module.HTTPException) as exc:
        main_module._validate_role("owner")

    assert exc.value.status_code == 400
