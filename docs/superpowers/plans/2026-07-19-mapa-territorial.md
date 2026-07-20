# Mapa Territorial Operacional Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Substituir o mapa limitado e dependente de CDN por uma visão operacional agregada por município, alimentada pelo PostGIS, com filtros, painel de processos e estados de falha confiáveis.

**Architecture:** O backend agrega processos por município e retorna coordenadas, contagens e maior oportunidade antes de aplicar o limite. O frontend importa Leaflet do bundle Vite, renderiza um marcador municipal por item e usa o endpoint de processos existente para preencher o painel lateral.

**Tech Stack:** FastAPI, SQLAlchemy, PostgreSQL/PostGIS, React 19, TypeScript strict, Vite 6, Leaflet 1.9.4, `@types/leaflet` 1.9.21, pytest, Node test runner, Docker Compose.

## Global Constraints

- Não exibir limites territoriais municipais ou estaduais.
- Coordenadas devem vir somente de `municipios_mt.geometry`.
- Calcular o ponto municipal com `ST_PointOnSurface`.
- Manter `/api/processos/mapa` para compatibilidade.
- Não manter coordenadas manuais no frontend.
- Não carregar Leaflet por CDN.
- Renderizar dados de processos como texto, sem concatenar HTML não escapado.
- Aplicar o limite a municípios, nunca antes da agregação dos processos.
- Exibir separadamente processos sem localização.
- Manter atribuição cartográfica visível.
- Usar OpenStreetMap somente no desenvolvimento local e manter o provedor configurável.
- Não adicionar MarkerCluster nesta fase.
- Painel lateral usa páginas de 10 processos.

---

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

### Task 2: Leaflet Local E Modelo Cartográfico

**Files:**
- Modify: `package.json`
- Modify: `package-lock.json`
- Modify: `frontend/src/types.ts`
- Create: `frontend/src/map/model.ts`
- Create: `frontend/tests/map-model.test.mjs`

**Interfaces:**
- Produces: `MapCitySummary`, `MapSummaryResponse`, `MapFilters`
- Produces: `buildMapSummaryParams(filters, region)`
- Produces: `markerTone(faixa)`

- [ ] **Step 1: Install fixed dependencies**

```bash
npm install leaflet@1.9.4
npm install --save-dev @types/leaflet@1.9.21
```

- [ ] **Step 2: Write failing model tests**

Update `frontend:test` to use Node TypeScript stripping:

```json
"frontend:test": "node --experimental-strip-types --test frontend/tests/*.test.mjs"
```

Create tests:

```javascript
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import test from "node:test";

const modelUrl = new URL("../src/map/model.ts", import.meta.url);

test("map model module exists", () => {
  assert.equal(existsSync(modelUrl), true);
});

if (existsSync(modelUrl)) {
  const { buildMapSummaryParams, markerTone } = await import(modelUrl.href);

  test("builds map query from local and global filters", () => {
    const params = buildMapSummaryParams({
      regiao: "",
      municipio: "Cuiabá",
      faixa: "janela_quente",
      dataInicio: "2026-01-01",
      dataFim: "",
    }, "Centro-Sul");
    assert.equal(params.get("regiao"), "Centro-Sul");
    assert.equal(params.get("municipio"), "Cuiabá");
    assert.equal(params.get("faixa"), "janela_quente");
    assert.equal(params.get("limit_cidades"), "200");
  });

  test("maps probability bands to stable marker tones", () => {
    assert.equal(markerTone("janela_quente"), "critical");
    assert.equal(markerTone("provavel"), "high");
    assert.equal(markerTone("observacao"), "medium");
    assert.equal(markerTone("frio"), "low");
  });
}
```

- [ ] **Step 3: Run model tests and verify RED**

```bash
npm run frontend:test
```

Expected: FAIL in `map model module exists` with `false !== true`.

- [ ] **Step 4: Add strict types**

```typescript
export type MapFilters = {
  regiao: string;
  municipio: string;
  faixa: string;
  dataInicio: string;
  dataFim: string;
};

export type MapCitySummary = {
  municipio: string;
  regiao_imea: string;
  lat: number;
  lng: number;
  total_processos: number;
  maior_score: number;
  processos_quentes: number;
  processos_provaveis: number;
  faixa_dominante: string;
  ultima_distribuicao: string;
};

export type MapSummaryResponse = {
  total_processos: number;
  total_municipios: number;
  sem_localizacao: number;
  items: MapCitySummary[];
};
```

- [ ] **Step 5: Implement model helpers**

```typescript
import type { MapFilters } from "../types";

export function buildMapSummaryParams(filters: MapFilters, globalRegion: string) {
  const params = new URLSearchParams({ limit_cidades: "200" });
  const region = globalRegion || filters.regiao;
  if (region) params.set("regiao", region);
  if (filters.municipio.trim()) params.set("municipio", filters.municipio.trim());
  if (filters.faixa) params.set("faixa", filters.faixa);
  if (filters.dataInicio) params.set("data_inicio", filters.dataInicio);
  if (filters.dataFim) params.set("data_fim", filters.dataFim);
  return params;
}

export function markerTone(faixa: string) {
  if (faixa === "janela_quente") return "critical";
  if (faixa === "provavel") return "high";
  if (faixa === "observacao") return "medium";
  return "low";
}
```

- [ ] **Step 6: Verify**

```bash
npm run frontend:test
npm run frontend:build
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add package.json package-lock.json frontend/src/types.ts frontend/src/map/model.ts frontend/tests/map-model.test.mjs
git commit -m "build: bundle leaflet for territorial map"
```

---

### Task 3: Reutilizar Detalhes E Acompanhamento

**Files:**
- Create: `frontend/src/components/ProcessModal.tsx`
- Modify: `frontend/src/screens/Processos.tsx`
- Modify: `frontend/src/main.tsx`

**Interfaces:**
- Produces: `<ProcessModal processo close follow />`
- MapScreen receives: `navigate`, `notify`

- [ ] **Step 1: Add a source contract test**

Create `frontend/tests/process-modal-contract.test.mjs`:

```javascript
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import test from "node:test";

const modalUrl = new URL("../src/components/ProcessModal.tsx", import.meta.url);

test("process modal exists and is shared by radar", async () => {
  assert.equal(existsSync(modalUrl), true);
  const modal = await readFile(modalUrl, "utf8");
  const radar = await readFile(
    new URL("../src/screens/Processos.tsx", import.meta.url),
    "utf8",
  );
  assert.match(modal, /export function ProcessModal/);
  assert.match(radar, /import \{ ProcessModal \}/);
});
```

- [ ] **Step 2: Verify RED**

```bash
npm run frontend:test
```

Expected: FAIL with `false !== true` before attempting to read the missing component.

- [ ] **Step 3: Extract the existing modal**

Move the current modal JSX and its imports (`BellPlus`, `X`, `shortDate`,
`scoreLabel`, `Processo`) into `ProcessModal.tsx`. Export it with:

```typescript
export function ProcessModal({
  processo,
  close,
  follow,
}: {
  processo: Processo;
  close: () => void;
  follow: (processo: Processo) => void;
}) { /* existing modal body */ }
```

Remove the private implementation from `Processos.tsx` and import the shared one.

- [ ] **Step 4: Pass navigation and notification into MapScreen**

```tsx
<MapScreen
  api={api}
  region={region}
  navigate={navigate}
  notify={notify}
/>
```

- [ ] **Step 5: Verify**

```bash
npm run frontend:test
npm run frontend:build
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/ProcessModal.tsx frontend/src/screens/Processos.tsx frontend/src/main.tsx frontend/tests/process-modal-contract.test.mjs
git commit -m "refactor: share process details modal"
```

---

### Task 4: Novo Mapa E Painel Municipal

**Files:**
- Replace: `frontend/src/screens/MapScreen.tsx`
- Modify: `frontend/src/styles.css`
- Modify: `frontend/tests/dashboard-quality.test.mjs`
- Create: `frontend/tests/map-screen-contract.test.mjs`

**Interfaces:**
- Consumes: `/api/processos/mapa/resumo`
- Consumes: `/api/processos?municipio=...&limit=10&offset=...`
- Consumes: `MapCitySummary`, `MapSummaryResponse`, `MapFilters`

- [ ] **Step 1: Write failing screen contract**

```javascript
test("map uses bundled leaflet and aggregated endpoint", async () => {
  const source = await readFile(
    new URL("../src/screens/MapScreen.tsx", import.meta.url),
    "utf8",
  );
  assert.match(source, /from "leaflet"/);
  assert.match(source, /leaflet\\/dist\\/leaflet\\.css/);
  assert.match(source, /\\/api\\/processos\\/mapa\\/resumo/);
  assert.doesNotMatch(source, /unpkg\\.com/);
  assert.doesNotMatch(source, /CITY_COORDS/);
});
```

- [ ] **Step 2: Verify RED**

```bash
npm run frontend:test
```

Expected: FAIL because the old CDN and `CITY_COORDS` still exist.

- [ ] **Step 3: Replace dynamic Leaflet loading**

Use static imports:

```typescript
import L from "leaflet";
import "leaflet/dist/leaflet.css";
```

Create the map once with:

```typescript
L.map(mapRef.current, {
  center: [-13.8, -55.9],
  zoom: 6,
  zoomControl: true,
  attributionControl: true,
});
```

Use:

```typescript
const tileUrl =
  import.meta.env.VITE_MAP_TILE_URL ||
  "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
```

Register `tileerror` to set `tilesAvailable` to `false`. Never switch to a
blank fallback list merely because tiles fail.

- [ ] **Step 4: Implement aggregated loading**

`loadSummary` must:

1. build parameters with `buildMapSummaryParams`;
2. fetch `/api/processos/mapa/resumo`;
3. replace counters and municipalities atomically;
4. clear selected municipality if it disappeared;
5. fit bounds only to finite coordinates;
6. preserve the neutral map background when no tiles are available.

- [ ] **Step 5: Render stable municipal markers**

For each item use `L.circleMarker`:

```typescript
const marker = L.circleMarker([city.lat, city.lng], {
  radius: 13,
  weight: selected ? 3 : 2,
  color: "#ffffff",
  fillColor: markerColor(markerTone(city.faixa_dominante)),
  fillOpacity: 1,
});
```

Bind a tooltip using a DOM node and `textContent`, never HTML strings. Add a
centered count label with `L.divIcon` or a permanent tooltip whose content is
also a DOM node. Clicking either layer selects the city.

- [ ] **Step 6: Implement filters and summary counters**

Use a compact filter bar with:

- probability range;
- region IMEA;
- municipality text;
- start date default `2026-01-01`;
- end date;
- clear button.

Show:

- localized processes;
- represented municipalities;
- missing location;
- selected region or `Mato Grosso`.

- [ ] **Step 7: Implement municipal side panel**

When a city is selected, fetch:

```text
/api/processos?municipio=<exact city>&limit=10&offset=<page*10>
```

and preserve active range, region and date filters. Render compact process rows
with score, class, CNJ and distribution date. Provide `Detalhes` and
`Acompanhar` actions. `Acompanhar` uses:

```typescript
await api.post(`/api/processos/${processo.id}/acompanhar`)
```

On success, notify and navigate to `alertas`.

- [ ] **Step 8: Implement responsive CSS**

Add fixed layout constraints:

```css
.map-workspace {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 360px;
  min-height: 620px;
}
.leaflet-map { min-height: 620px; height: calc(100vh - 270px); }
.map-side-panel { min-width: 0; border-left: 1px solid var(--line); }
@media (max-width: 1180px) {
  .map-workspace { grid-template-columns: 1fr; }
  .map-side-panel { border-left: 0; border-top: 1px solid var(--line); }
  .leaflet-map { min-height: 480px; height: 58vh; }
}
```

Add legend, marker, selected marker, tile warning, compact rows, empty and
loading states using the existing color tokens. No shadows on markers.

- [ ] **Step 9: Verify**

```bash
npm run frontend:test
npm run frontend:build
```

Expected: PASS and no TypeScript errors.

- [ ] **Step 10: Commit**

```bash
git add frontend/src/screens/MapScreen.tsx frontend/src/styles.css frontend/tests
git commit -m "feat: redesign territorial process map"
```

---

### Task 5: Docker, Dados Reais E Regressão

**Files:**
- Modify: `tools/smoke_test.py`
- Modify: `docs/PENDENCIAS_PRODUCAO.md`
- Modify: `docs/DEPLOY_PRODUCAO.md`

**Interfaces:**
- Smoke check: authenticated `/api/processos/mapa/resumo`

- [ ] **Step 1: Extend smoke test**

Add:

```python
("mapa processos", "/api/processos/mapa/resumo?limit_cidades=200"),
```

Validate that the returned payload contains:

```python
{"total_processos", "total_municipios", "sem_localizacao", "items"}
```

- [ ] **Step 2: Run smoke test source checks**

```bash
python -m py_compile tools/smoke_test.py api/main.py database/db.py
```

Expected: PASS.

- [ ] **Step 3: Rebuild all application services**

```bash
docker compose up -d --build web worker beat
docker compose ps
```

Expected: web, worker and beat `Up`; web, db and redis healthy.

- [ ] **Step 4: Run complete backend suite against local code**

```bash
docker compose run --rm -T -v "${PWD}:/app" -w /app web \
  sh -c "pip install --user -r requirements-dev.txt >/tmp/pip-test.log && \
  RUN_POSTGIS_INTEGRATION=true APP_ENV=test python -m pytest"
```

Expected: all tests PASS, including PostGIS integration.

- [ ] **Step 5: Validate real map totals**

Authenticated response must match database evidence:

- `total_processos`: 702 for the current unfiltered database;
- `total_municipios`: 73;
- `sem_localizacao`: 27.

If collection data changes during implementation, compare endpoint results with
fresh SQL counts rather than forcing these historical numbers.

- [ ] **Step 6: Run authenticated smoke**

```bash
docker compose exec -T web sh -c \
  'RADAR_SMOKE_USER=admin RADAR_SMOKE_PASSWORD="$DEFAULT_ADMIN_PASSWORD" \
  python tools/smoke_test.py --base-url http://127.0.0.1:8000'
```

Expected: every check `[OK]`.

- [ ] **Step 7: Visual validation**

Validate `http://localhost:8000` at:

- 1440x900;
- 1024x768;
- 390x844.

Check:

- basemap is not blank;
- markers are visible and compact;
- counts match the endpoint;
- selection opens the correct city;
- detail and follow actions work;
- tile failure warning does not remove process data;
- no polygons or territorial limits appear;
- no text or controls overlap.

- [ ] **Step 8: Update documentation**

Record:

- Leaflet is bundled;
- map endpoint aggregates before limiting;
- provider tile URL is configurable;
- OSM development-only caveat;
- current real totals from the final validation.

- [ ] **Step 9: Final verification**

```bash
git diff --check
npm run frontend:test
npm run frontend:build
```

Expected: exit code 0.

- [ ] **Step 10: Commit**

```bash
git add tools/smoke_test.py docs/PENDENCIAS_PRODUCAO.md docs/DEPLOY_PRODUCAO.md
git commit -m "docs: document territorial map operations"
```
