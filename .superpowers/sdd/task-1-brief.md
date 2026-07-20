### Task 1: Endpoint PostGIS Agregado

**Files:**
- Modify: `database/db.py`
- Modify: `api/main.py`
- Modify: `tests/test_database_methods.py`
- Modify: `tests/test_api_testclient.py`

**Interfaces:**
- Produces: `Database.resumo_mapa_processos(filtros: dict, limit_cidades: int) -> dict`
- Produces: `GET /api/processos/mapa/resumo`
- Response keys: `total_processos`, `total_municipios`, `sem_localizacao`, `items`

- [ ] **Step 1: Write failing database tests**

Add a test that monkeypatches `db.query` with three sequential DataFrames:

```python
def test_resumo_mapa_processos_aggregates_before_city_limit(monkeypatch):
    db = Database.__new__(Database)
    responses = iter([
        pd.DataFrame([{
            "municipio": "Cuiabá",
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
        {"regiao": "Centro-Sul", "faixa": "janela_quente"},
        limit_cidades=100,
    )

    assert result["total_processos"] == 702
    assert result["sem_localizacao"] == 27
    assert result["total_municipios"] == 73
    assert result["items"][0]["total_processos"] == 152
    assert "GROUP BY" in calls[0][0]
    assert calls[0][1]["limit_cidades"] == 100
```

- [ ] **Step 2: Run the database test and verify RED**

Run:

```bash
python -m pytest tests/test_database_methods.py::test_resumo_mapa_processos_aggregates_before_city_limit -q
```

Expected: FAIL because `resumo_mapa_processos` does not exist.

- [ ] **Step 3: Implement the database method**

Add a method that builds only named-parameter predicates:

```python
def resumo_mapa_processos(self, filtros: dict, limit_cidades: int = 200) -> dict:
    where_parts = []
    params = {"limit_cidades": max(1, min(int(limit_cidades), 200))}
    mapping = {
        "regiao": ("p.regiao_imea = :regiao", "regiao"),
        "faixa": ("s.faixa_probabilidade = :faixa", "faixa"),
        "data_inicio": ("p.data_distribuicao >= :data_inicio", "data_inicio"),
        "data_fim": ("p.data_distribuicao <= :data_fim", "data_fim"),
    }
    for key, (clause, param_name) in mapping.items():
        value = filtros.get(key)
        if value:
            where_parts.append(clause)
            params[param_name] = value
    if filtros.get("municipio"):
        where_parts.append("p.municipio ILIKE :municipio")
        params["municipio"] = f"%{filtros['municipio']}%"
    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    items = self.query(f"""
        WITH filtrados AS (
            SELECT p.id, p.municipio, p.regiao_imea, p.data_distribuicao,
                   s.score_total, s.faixa_probabilidade, m.geometry
            FROM processos p
            LEFT JOIN score_pericial s ON s.processo_id = p.id
            LEFT JOIN municipios_mt m ON lower(m.nome) = lower(p.municipio)
            {where}
        )
        SELECT municipio,
               MAX(regiao_imea) AS regiao_imea,
               ST_Y(ST_PointOnSurface(geometry)) AS lat,
               ST_X(ST_PointOnSurface(geometry)) AS lng,
               COUNT(*)::int AS total_processos,
               COALESCE(MAX(score_total), 0)::int AS maior_score,
               COUNT(*) FILTER (WHERE faixa_probabilidade = 'janela_quente')::int AS processos_quentes,
               COUNT(*) FILTER (WHERE faixa_probabilidade = 'provavel')::int AS processos_provaveis,
               (ARRAY_AGG(COALESCE(faixa_probabilidade, 'frio')
                  ORDER BY COALESCE(score_total, 0) DESC))[1] AS faixa_dominante,
               MAX(data_distribuicao)::text AS ultima_distribuicao
        FROM filtrados
        WHERE geometry IS NOT NULL
        GROUP BY municipio, geometry
        ORDER BY maior_score DESC, total_processos DESC, municipio
        LIMIT :limit_cidades
    """, params)
    count_params = {k: v for k, v in params.items() if k != "limit_cidades"}
    totals = self.query(f"""
        SELECT COUNT(*) FILTER (WHERE m.geometry IS NOT NULL)::int AS total_processos,
               COUNT(DISTINCT p.municipio)
                   FILTER (WHERE m.geometry IS NOT NULL)::int AS total_municipios,
               COUNT(*) FILTER (WHERE m.geometry IS NULL)::int AS sem_localizacao
        FROM processos p
        LEFT JOIN score_pericial s ON s.processo_id = p.id
        LEFT JOIN municipios_mt m ON lower(m.nome) = lower(p.municipio)
        {where}
    """, count_params).iloc[0]
    clean = items.fillna("").to_dict(orient="records")
    return {
        "total_processos": int(totals["total_processos"]),
        "total_municipios": int(totals["total_municipios"]),
        "sem_localizacao": int(totals["sem_localizacao"]),
        "items": clean,
    }
```


- [ ] **Step 4: Add failing API tests**

Add `resumo_mapa_processos` to `FakeDb`, recording received filters, and add:

```python
def test_processos_mapa_resumo_returns_aggregated_cities(api_client):
    client, fake_db = api_client
    response = client.get(
        "/api/processos/mapa/resumo?regiao=Centro-Sul&faixa=janela_quente"
        "&data_inicio=2026-01-01&limit_cidades=100",
        headers=auth_header("token-viewer"),
    )
    payload = response.json()

    assert response.status_code == 200
    assert payload["total_processos"] == 702
    assert payload["total_municipios"] == 1
    assert payload["items"][0]["municipio"] == "Cuiabá"
    assert fake_db.map_filters["regiao"] == "Centro-Sul"
    assert fake_db.map_filters["faixa"] == "janela_quente"
```

- [ ] **Step 5: Run the API test and verify RED**

Run:

```bash
python -m pytest tests/test_api_testclient.py::test_processos_mapa_resumo_returns_aggregated_cities -q
```

Expected: FAIL with 404.

- [ ] **Step 6: Implement the FastAPI endpoint**

```python
@app.get("/api/processos/mapa/resumo")
async def processos_mapa_resumo(
    regiao: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    faixa: Optional[str] = Query(None),
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    limit_cidades: int = Query(200, ge=1, le=200),
    _user: AuthUser = None,
):
    if not _db:
        return {
            "total_processos": 0,
            "total_municipios": 0,
            "sem_localizacao": 0,
            "items": [],
        }
    filtros = {
        "regiao": regiao,
        "municipio": municipio,
        "faixa": faixa,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
    }
    return _db.resumo_mapa_processos(filtros, limit_cidades)
```

Wrap unexpected exceptions with logging and a `500` response rather than returning a false empty success.

- [ ] **Step 7: Run backend tests**

```bash
python -m pytest tests/test_database_methods.py tests/test_api_testclient.py -q
```

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add api/main.py database/db.py tests/test_api_testclient.py tests/test_database_methods.py
git commit -m "feat: aggregate process map by municipality"
```

---

