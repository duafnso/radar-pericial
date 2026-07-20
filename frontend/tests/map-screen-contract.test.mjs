import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const screenUrl = new URL("../src/screens/MapScreen.tsx", import.meta.url);
const modelUrl = new URL("../src/map/model.ts", import.meta.url);
const stylesUrl = new URL("../src/styles.css", import.meta.url);

test("map uses bundled leaflet and aggregated endpoint", async () => {
  const source = await readFile(screenUrl, "utf8");

  assert.match(source, /from "leaflet"/);
  assert.match(source, /leaflet\/dist\/leaflet\.css/);
  assert.match(source, /\/api\/processos\/mapa\/resumo/);
  assert.match(source, /buildMapSummaryParams/);
  assert.doesNotMatch(source, /unpkg\.com/);
  assert.doesNotMatch(source, /CITY_COORDS/);
});

test("map tiles are configurable, attributed, and fail without hiding data", async () => {
  const source = await readFile(screenUrl, "utf8");
  const model = await readFile(modelUrl, "utf8");

  assert.match(source, /VITE_MAP_TILE_URL/);
  assert.match(source, /VITE_MAP_TILE_ATTRIBUTION/);
  assert.match(source, /resolveTileConfig/);
  assert.match(model, /https:\/\/tile\.openstreetmap\.org\/\{z\}\/\{x\}\/\{y\}\.png/);
  assert.match(model, /Configuração de tiles incompleta; usando OpenStreetMap/);
  assert.match(source, /tileerror/);
  assert.match(source, /setTilesAvailable\(false\)/);
  assert.doesNotMatch(source, /tileLayer\.on\("load"/);
  assert.match(source, /attributionControl:\s*true/);
  assert.match(source, /map-tile-warning/);
});

test("map renders one stable safe marker per aggregated city", async () => {
  const source = await readFile(screenUrl, "utf8");

  assert.match(source, /L\.circleMarker/);
  assert.match(source, /radius:\s*13/);
  assert.match(source, /markerTone\(city\.faixa_dominante\)/);
  assert.match(source, /textContent/);
  assert.match(source, /L\.divIcon/);
  assert.match(source, /aria-label/);
  assert.match(source, /event\.key !== "Enter"/);
  assert.match(source, /event\.key !== " "/);
  assert.match(source, /removeEventListener\("keydown"/);
  assert.doesNotMatch(source, /bindPopup\(\s*`/);
  assert.doesNotMatch(source, /L\.polygon|L\.geoJSON/);
});

test("municipal panel paginates ten filtered processes and reuses actions", async () => {
  const source = await readFile(screenUrl, "utf8");

  assert.match(source, /limit:\s*"10"/);
  assert.match(source, /offset:\s*String\(page \* PAGE_SIZE\)/);
  assert.match(source, /const selectedMunicipio = city\.municipio/);
  assert.match(source, /params\.set\("municipio",\s*selectedMunicipio\)/);
  assert.match(source, /params\.set\("data_inicio"/);
  assert.match(source, /params\.set\("data_fim"/);
  assert.match(source, /<ProcessModal/);
  assert.match(source, /\/acompanhar/);
  assert.match(source, /navigate\("alertas"\)/);
  assert.match(source, /notify\(/);
  assert.match(source, /setProcessRefresh\(\(current\) => current \+ 1\)/);
  assert.match(source, /parseProcessListResponse/);
  assert.match(source, /followInFlightRef/);
  assert.match(source, /formatCivilDate/);
});

test("map lifecycle cleans Leaflet layers, listeners, and map instance", async () => {
  const source = await readFile(screenUrl, "utf8");

  assert.match(source, /\.clearLayers\(\)/);
  assert.match(source, /\.off\("tileerror"/);
  assert.match(source, /\.remove\(\)/);
  assert.match(source, /Number\.isFinite\(city\.lat\)/);
  assert.match(source, /Number\.isFinite\(city\.lng\)/);
});

test("territorial workspace has stable responsive layout and marker styling", async () => {
  const styles = await readFile(stylesUrl, "utf8");

  assert.match(styles, /\.map-workspace\s*\{/);
  assert.match(styles, /grid-template-columns:\s*minmax\(0,\s*1fr\)\s+360px/);
  assert.match(styles, /\.leaflet-map\s*\{[^}]*min-height:\s*620px/s);
  assert.match(styles, /\.map-side-panel\s*\{[^}]*min-width:\s*0/s);
  assert.match(styles, /\.map-city-count\s*\{[^}]*box-shadow:\s*none/s);
  assert.match(styles, /@media\s*\(max-width:\s*1180px\)[\s\S]*\.map-workspace\s*\{[^}]*grid-template-columns:\s*1fr/);
  assert.match(styles, /\.map-overlay-stack\s*\{[^}]*display:\s*grid[^}]*gap:\s*6px/s);
  assert.match(styles, /\.map-score-chip\.critical\s*\{[^}]*#12492a/s);
  assert.match(styles, /\.map-score-chip\.high\s*\{[^}]*#17613a/s);
  assert.match(styles, /\.map-score-chip\.medium\s*\{[^}]*#256b45/s);
  assert.match(styles, /\.map-score-chip\.low\s*\{[^}]*#3d6449/s);
  assert.match(styles, /@media\s*\(max-width:\s*820px\)[\s\S]*\.map-filter-bar/);
  const mapStyles = styles.slice(styles.indexOf(".map-filter-bar"), styles.indexOf(".toast"));
  assert.doesNotMatch(mapStyles, /letter-spacing:\s*\.(?=\d*[1-9])/);
});
