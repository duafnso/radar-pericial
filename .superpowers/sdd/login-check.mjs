import fs from "node:fs";
import path from "node:path";
import { chromium } from "playwright-core";

const env = Object.fromEntries(fs.readFileSync(".env", "utf8").split(/\r?\n/).map((line) => line.trim()).filter((line) => line && !line.startsWith("#") && line.includes("=")).map((line) => {
  const index = line.indexOf("=");
  return [line.slice(0, index).trim(), line.slice(index + 1).trim().replace(/^["']|["']$/g, "")];
}));
const username = env.DEFAULT_ADMIN_USERNAME || "admin";
const password = env.DEFAULT_ADMIN_PASSWORD;
if (!password) throw new Error("Credencial local ausente");

const browser = await chromium.launch({ executablePath: "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe", headless: true });
const page = await browser.newPage({ viewport: { width: 1280, height: 800 } });
const unexpectedFailures = [];
page.on("response", (response) => {
  if (response.status() >= 400 && !(response.status() === 401 && response.url().endsWith("/api/login"))) {
    unexpectedFailures.push({ status: response.status(), url: response.url() });
  }
});
await page.goto("http://127.0.0.1:8000/", { waitUntil: "networkidle" });
const logo = page.getByRole("img", { name: "Radar Pericial" });
await logo.waitFor();
const logoState = await logo.evaluate((element) => ({ src: element.getAttribute("src"), loaded: element.naturalWidth > 0 }));
await page.getByLabel("Usuário").fill(username);
await page.getByLabel("Senha").fill("senha-incorreta-teste");
await page.getByRole("button", { name: "Entrar" }).click();
const errorText = await page.getByRole("alert").textContent();
await page.getByLabel("Senha").fill(password);
await page.getByRole("button", { name: "Entrar" }).click();
await page.getByRole("button", { name: "Painel" }).waitFor();
console.log(JSON.stringify({ logoState, errorText, authenticated: true, unexpectedFailures }, null, 2));
await browser.close();
