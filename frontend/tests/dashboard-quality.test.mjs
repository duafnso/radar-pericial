import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const dashboardPath = new URL("../src/screens/Dashboard.tsx", import.meta.url);

test("dashboard exposes the process data quality summary", async () => {
  const source = await readFile(dashboardPath, "utf8");

  assert.match(source, /\/api\/qualidade\/processos/);
  assert.match(source, /Qualidade dos dados/);
  assert.match(source, /score_qualidade/);
  assert.match(source, /navigate\("mapa"\)/);
});
