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
