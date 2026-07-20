"""
Smoke test operacional do Radar Pericial.

Uso:
    python tools/smoke_test.py --base-url http://localhost:8000 --username admin

A senha pode ser passada por --password ou pela variavel RADAR_SMOKE_PASSWORD.
"""

import argparse
import json
import os
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


def _request(
    base_url: str,
    path: str,
    method: str = "GET",
    token: str | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=20) as response:
            raw = response.read()
            content_type = response.headers.get("content-type", "")
            if "application/json" in content_type:
                payload = json.loads(raw.decode("utf-8") or "{}")
            else:
                payload = raw.decode("utf-8", errors="replace")
            return response.status, payload
    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = raw
        return exc.code, payload
    except URLError as exc:
        raise RuntimeError(f"Falha de conexao com {url}: {exc.reason}") from exc


def _check(name: str, condition: bool, detail: str = "") -> None:
    status = "OK" if condition else "FAIL"
    suffix = f" - {detail}" if detail else ""
    print(f"[{status}] {name}{suffix}")
    if not condition:
        raise AssertionError(name)


def _redact(value: Any) -> str:
    if isinstance(value, dict):
        safe = {}
        for key, item in value.items():
            if key.lower() in {"token", "authorization", "password", "secret"}:
                safe[key] = "***"
            else:
                safe[key] = item
        return str(safe)
    return str(value)


def run(base_url: str, username: str, password: str) -> None:
    status, health = _request(base_url, "/health")
    _check("health", status == 200 and health.get("status") == "healthy", str(health))

    status, ready = _request(base_url, "/health/ready")
    _check("readiness", status == 200 and ready.get("status") == "ready", str(ready))

    status, html = _request(base_url, "/")
    _check("frontend html", status == 200 and "/static/assets/" in html)

    asset_marker = "/static/assets/"
    first_asset = html.split(asset_marker, 1)[1].split('"', 1)[0].split("'", 1)[0]
    status, _asset = _request(base_url, f"/static/assets/{first_asset}")
    _check("frontend asset", status == 200, f"/static/assets/{first_asset}")

    status, login = _request(
        base_url,
        "/api/login",
        method="POST",
        body={"username": username, "password": password},
    )
    _check("login", status == 200 and login.get("status") == "ok", _redact(login))
    token = login["token"]

    checks = [
        ("me", "/api/me"),
        ("stats", "/api/stats"),
        ("processos", "/api/processos?" + urlencode({"limit": 5})),
        ("coletas status", "/api/coletas/status?" + urlencode({"limit": 5})),
        ("coletas resumo", "/api/coletas/resumo"),
        ("qualidade processos", "/api/qualidade/processos"),
    ]
    for name, path in checks:
        status, payload = _request(base_url, path, token=token)
        _check(name, status == 200, str(payload)[:200])


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test do Radar Pericial")
    parser.add_argument("--base-url", default=os.getenv("RADAR_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--username", default=os.getenv("RADAR_SMOKE_USERNAME") or os.getenv("RADAR_SMOKE_USER", "admin"))
    parser.add_argument("--password", default=os.getenv("RADAR_SMOKE_PASSWORD"))
    args = parser.parse_args()

    if not args.password:
        print("Defina --password ou RADAR_SMOKE_PASSWORD.", file=sys.stderr)
        return 2

    try:
        run(args.base_url, args.username, args.password)
    except Exception as exc:
        print(f"Smoke test falhou: {exc}", file=sys.stderr)
        return 1
    print("Smoke test concluido com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
