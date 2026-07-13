import requests


def datajud_hit(numero="0000001-00.2026.8.11.0001"):
    return {
        "_source": {
            "numeroProcesso": numero,
            "classe": {"nome": "Desapropriacao"},
            "tribunal": {"sigla": "TJMT"},
            "orgaoJulgador": {"nome": "Vara Agraria - Comarca de Cuiaba"},
            "assuntos": [{"nome": "Imovel rural"}],
            "dataAjuizamento": "2026-07-01T10:30:00",
            "movimentos": [{"nome": "Nomeacao de perito"}],
        }
    }


def test_fetch_datajud_200_normalizes_process(requests_mock, monkeypatch):
    import collector.judicial_collector as collector

    monkeypatch.setenv("DATAJUD_PAGE_SIZE", "50")
    monkeypatch.setenv("DATAJUD_MAX_RESULTS_PER_CLASS", "50")
    monkeypatch.setenv("DATAJUD_REQUEST_DELAY_SECONDS", "0")
    monkeypatch.setenv("DATAJUD_START_DATE", "2026-01-01")

    requests_mock.post(
        "https://api-publica.datajud.cnj.jus.br/api_publica_tjmt/_search",
        json={"hits": {"hits": [datajud_hit()]}},
        status_code=200,
    )

    processos = collector.fetch_datajud("Desapropriacao", dias_atras=1, max_results=50)

    assert len(processos) == 1
    sent = requests_mock.last_request.json()
    assert sent["query"]["bool"]["must"][0]["range"]["dataAjuizamento"]["gte"] == "2026-01-01"
    assert processos[0]["numero_cnj"] == "0000001-00.2026.8.11.0001"
    assert processos[0]["tribunal"] == "TJMT"
    assert processos[0]["municipio"] == "Cuiaba"
    assert processos[0]["data_distribuicao"] == "2026-07-01"
    assert processos[0]["_movimentacoes"] == ["Nomeacao de perito"]


def test_fetch_datajud_401_returns_empty_list(requests_mock, monkeypatch):
    import collector.judicial_collector as collector

    monkeypatch.setenv("DATAJUD_REQUEST_DELAY_SECONDS", "0")
    requests_mock.post(
        "https://api-publica.datajud.cnj.jus.br/api_publica_tjmt/_search",
        json={"error": "unauthorized"},
        status_code=401,
    )

    assert collector.fetch_datajud("Desapropriacao", dias_atras=1) == []


def test_fetch_datajud_429_returns_empty_list(requests_mock, monkeypatch):
    import collector.judicial_collector as collector

    monkeypatch.setenv("DATAJUD_REQUEST_DELAY_SECONDS", "0")
    monkeypatch.setenv("DATAJUD_429_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv("DATAJUD_MAX_429_RETRIES", "0")
    requests_mock.post(
        "https://api-publica.datajud.cnj.jus.br/api_publica_tjmt/_search",
        json={"error": "too_many_requests"},
        status_code=429,
    )

    assert collector.fetch_datajud("Desapropriacao", dias_atras=1) == []


def test_fetch_datajud_explicit_start_date_overrides_environment(requests_mock, monkeypatch):
    import collector.judicial_collector as collector

    monkeypatch.setenv("DATAJUD_START_DATE", "2026-01-01")
    monkeypatch.setenv("DATAJUD_REQUEST_DELAY_SECONDS", "0")
    requests_mock.post(
        "https://api-publica.datajud.cnj.jus.br/api_publica_tjmt/_search",
        json={"hits": {"hits": []}},
        status_code=200,
    )

    collector.fetch_datajud("Desapropriacao", dias_atras=1, data_inicio="2026-07-01")

    sent = requests_mock.last_request.json()
    assert sent["query"]["bool"]["must"][0]["range"]["dataAjuizamento"]["gte"] == "2026-07-01"


def test_fetch_datajud_timeout_returns_empty_list(monkeypatch):
    import collector.judicial_collector as collector

    def raise_timeout(*args, **kwargs):
        raise requests.Timeout("DataJud timed out")

    monkeypatch.setenv("DATAJUD_REQUEST_DELAY_SECONDS", "0")
    monkeypatch.setattr(collector.S, "post", raise_timeout)

    assert collector.fetch_datajud("Desapropriacao", dias_atras=1) == []


def test_judicial_collector_deduplicates_by_cnj(monkeypatch):
    import collector.judicial_collector as collector

    calls = []

    def fake_fetch_datajud(classe, dias_atras=30, max_results=100):
        calls.append(classe)
        return [
            {"numero_cnj": "0000001-00.2026.8.11.0001", "classe_processual": classe},
            {"numero_cnj": "0000001-00.2026.8.11.0001", "classe_processual": "duplicado"},
            {"numero_cnj": f"000000{len(calls) + 1}-00.2026.8.11.0001", "classe_processual": classe},
            {"classe_processual": "sem cnj"},
        ]

    monkeypatch.setattr(collector, "fetch_datajud", fake_fetch_datajud)
    monkeypatch.setattr(collector, "fetch_dje_tjmt", lambda dias_atras=7: [])
    monkeypatch.setattr(collector.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(collector, "_env_enabled", lambda name, default=True: name == "ENABLE_SOURCE_DATAJUD")
    monkeypatch.setattr(collector, "_env_int", lambda name, default: default)

    result = collector.JudicialCollector().run(dias_atras=1)

    cnjs = [p["numero_cnj"] for p in result["processos"]]
    assert len(calls) == 4
    assert len(cnjs) == len(set(cnjs))
    assert "0000001-00.2026.8.11.0001" in cnjs
    assert result["publicacoes_dje"] == []
