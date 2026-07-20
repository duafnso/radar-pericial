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

