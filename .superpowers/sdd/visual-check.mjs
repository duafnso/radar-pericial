import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright-core";

const root = process.cwd();
const envText = fs.readFileSync(path.join(root, ".env"), "utf8");
const env = Object.fromEntries(
  envText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#") && line.includes("="))
    .map((line) => {
      const index = line.indexOf("=");
      const key = line.slice(0, index).trim();
      const value = line.slice(index + 1).trim().replace(/^["']|["']$/g, "");
      return [key, value];
    }),
);

const username = env.DEFAULT_ADMIN_USERNAME || "admin";
const password = env.DEFAULT_ADMIN_PASSWORD;
if (!password) throw new Error("DEFAULT_ADMIN_PASSWORD ausente no .env local");

const outputDir = path.join(root, ".superpowers", "sdd", "screenshots");
fs.mkdirSync(outputDir, { recursive: true });

const browser = await chromium.launch({
  executablePath: "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
  headless: true,
});
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const consoleErrors = [];
const pageErrors = [];
page.on("console", (message) => {
  if (message.type() === "error") consoleErrors.push(message.text());
});
page.on("pageerror", (error) => pageErrors.push(error.message));

await page.goto("http://127.0.0.1:8000/", { waitUntil: "networkidle" });
await page.locator('input[autocomplete="username"]').fill(username);
await page.locator('input[autocomplete="current-password"]').fill(password);
await page.getByRole("button", { name: "Entrar" }).click();
await page.getByRole("button", { name: /Mapa Territorial/i }).waitFor();
await page.getByRole("button", { name: /Mapa Territorial/i }).click();
await page.locator(".map-summary-strip").waitFor();
await page.waitForFunction(() => document.querySelectorAll(".map-city-count").length > 0);
await page.waitForTimeout(1500);

const desktopMetrics = await page.evaluate(() => {
  const markers = [...document.querySelectorAll(".map-city-count")];
  const rects = markers.map((marker) => marker.getBoundingClientRect());
  const map = document.querySelector(".leaflet-map")?.getBoundingClientRect();
  const panel = document.querySelector(".map-side-panel")?.getBoundingClientRect();
  return {
    markerCount: markers.length,
    markerSizes: rects.slice(0, 10).map(({ width, height }) => ({ width, height })),
    markerLabels: markers.slice(0, 3).map((marker) => marker.getAttribute("aria-label")),
    map: map && { width: map.width, height: map.height },
    panel: panel && { width: panel.width, height: panel.height },
    polygons: document.querySelectorAll("path.leaflet-interactive").length,
    tileImages: document.querySelectorAll(".leaflet-tile-loaded").length,
  };
});
await page.screenshot({ path: path.join(outputDir, "mapa-desktop-1440x900.png"), fullPage: true });

await page.locator(".map-city-count").first().click();
await page.locator(".map-process-list, .map-panel-loading").first().waitFor();
await page.waitForTimeout(1000);
const selectedMetrics = await page.evaluate(() => ({
  selectedMarkers: document.querySelectorAll(".map-city-count.is-selected").length,
  processRows: document.querySelectorAll(".map-process-row").length,
  panelLabel: document.querySelector(".map-side-panel")?.getAttribute("aria-label"),
  horizontalOverflow: document.documentElement.scrollWidth > document.documentElement.clientWidth,
}));
await page.screenshot({ path: path.join(outputDir, "mapa-municipio-selecionado.png"), fullPage: true });

const responsive = [];
for (const viewport of [
  { width: 1024, height: 768, name: "mapa-notebook-1024x768.png" },
  { width: 390, height: 844, name: "mapa-mobile-390x844.png" },
]) {
  await page.setViewportSize({ width: viewport.width, height: viewport.height });
  await page.waitForTimeout(500);
  const metrics = await page.evaluate(() => ({
    width: window.innerWidth,
    horizontalOverflow: document.documentElement.scrollWidth > document.documentElement.clientWidth,
    mapRect: (() => {
      const rect = document.querySelector(".leaflet-map")?.getBoundingClientRect();
      return rect && { width: rect.width, height: rect.height };
    })(),
    panelRect: (() => {
      const rect = document.querySelector(".map-side-panel")?.getBoundingClientRect();
      return rect && { width: rect.width, height: rect.height };
    })(),
  }));
  responsive.push(metrics);
  await page.screenshot({ path: path.join(outputDir, viewport.name), fullPage: true });
}

const result = {
  desktopMetrics,
  selectedMetrics,
  responsive,
  consoleErrors,
  pageErrors,
};
fs.writeFileSync(
  path.join(outputDir, "visual-check.json"),
  JSON.stringify(result, null, 2),
  "utf8",
);
console.log(JSON.stringify(result, null, 2));
await browser.close();
