import { cpSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, extname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const dist = join(root, "frontend", "dist");
const targetTemplate = join(root, "interface", "templates", "index.html");
const targetAssets = join(root, "interface", "static", "assets");
const textExtensions = new Set([".css", ".html", ".js", ".svg"]);

function normalizeTextFiles(directory) {
  for (const name of readdirSync(directory)) {
    const path = join(directory, name);
    if (statSync(path).isDirectory()) {
      normalizeTextFiles(path);
    } else if (textExtensions.has(extname(path))) {
      const normalized = readFileSync(path, "utf8").replace(/\r\n?/g, "\n");
      writeFileSync(path, normalized, "utf8");
    }
  }
}

mkdirSync(dirname(targetTemplate), { recursive: true });
mkdirSync(join(root, "interface", "static"), { recursive: true });
rmSync(targetAssets, { recursive: true, force: true });
cpSync(join(dist, "assets"), targetAssets, { recursive: true });
normalizeTextFiles(targetAssets);

const html = readFileSync(join(dist, "index.html"), "utf8")
  .replace(/\r\n?/g, "\n")
  .replace(/\n\s*\n(?=\s*<\/body>)/, "\n")
  .replaceAll('src="/assets/', 'src="/static/assets/')
  .replaceAll('href="/assets/', 'href="/static/assets/');

writeFileSync(targetTemplate, html, "utf8");
