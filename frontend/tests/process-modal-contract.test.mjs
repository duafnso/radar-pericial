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
  assert.match(
    radar,
    /<ProcessModal\s+processo=\{selected\}\s+close=\{\(\) => setSelected\(null\)\}\s+follow=\{follow\}\s*\/>/,
  );
});

test("process modal preserves close and follow action wiring", async () => {
  const modal = await readFile(modalUrl, "utf8");
  const closeActions = modal.match(/onClick=\{close\}/g) || [];

  assert.equal(closeActions.length, 3);
  assert.match(modal, /onClick=\{\(\) => follow\(processo\)\}/);
});

test("process modal manages keyboard focus as a modal dialog", async () => {
  const modal = await readFile(modalUrl, "utf8");

  assert.match(modal, /document\.activeElement/);
  assert.match(modal, /\.focus\(\)/);
  assert.match(modal, /event\.key === "Escape"/);
  assert.match(modal, /event\.key !== "Tab"/);
  assert.match(modal, /event\.shiftKey/);
  assert.match(modal, /addEventListener\("keydown"/);
  assert.match(modal, /removeEventListener\("keydown"/);
  assert.match(modal, /previouslyFocused\.focus\(\)/);
});

test("map screen receives navigation and notification callbacks", async () => {
  const main = await readFile(new URL("../src/main.tsx", import.meta.url), "utf8");

  assert.match(
    main,
    /<MapScreenWithFutureProps\s+api=\{api\}\s+region=\{region\}\s+navigate=\{navigate\}\s+notify=\{notify\}\s*\/>/,
  );
});
