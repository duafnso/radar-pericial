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

