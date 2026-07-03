import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const dist = join(root, "frontend", "dist");
const targetTemplate = join(root, "interface", "templates", "index.html");
const targetAssets = join(root, "interface", "static", "assets");

mkdirSync(dirname(targetTemplate), { recursive: true });
mkdirSync(join(root, "interface", "static"), { recursive: true });
rmSync(targetAssets, { recursive: true, force: true });
cpSync(join(dist, "assets"), targetAssets, { recursive: true });

const html = readFileSync(join(dist, "index.html"), "utf8")
  .replaceAll('src="/assets/', 'src="/static/assets/')
  .replaceAll('href="/assets/', 'href="/static/assets/');

writeFileSync(targetTemplate, html, "utf8");
