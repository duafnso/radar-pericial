### Task 3: Reutilizar Detalhes E Acompanhamento

**Files:**
- Create: `frontend/src/components/ProcessModal.tsx`
- Modify: `frontend/src/screens/Processos.tsx`
- Modify: `frontend/src/main.tsx`

**Interfaces:**
- Produces: `<ProcessModal processo close follow />`
- MapScreen receives: `navigate`, `notify`

- [ ] **Step 1: Add a source contract test**

Create `frontend/tests/process-modal-contract.test.mjs`:

```javascript
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
```

- [ ] **Step 2: Verify RED**

```bash
npm run frontend:test
```

Expected: FAIL with `false !== true` before attempting to read the missing component.

- [ ] **Step 3: Extract the existing modal**

Move the current modal JSX and its imports (`BellPlus`, `X`, `shortDate`,
`scoreLabel`, `Processo`) into `ProcessModal.tsx`. Export it with:

```typescript
export function ProcessModal({
  processo,
  close,
  follow,
}: {
  processo: Processo;
  close: () => void;
  follow: (processo: Processo) => void;
}) { /* existing modal body */ }
```

Remove the private implementation from `Processos.tsx` and import the shared one.

- [ ] **Step 4: Pass navigation and notification into MapScreen**

```tsx
<MapScreen
  api={api}
  region={region}
  navigate={navigate}
  notify={notify}
/>
```

- [ ] **Step 5: Verify**

```bash
npm run frontend:test
npm run frontend:build
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/ProcessModal.tsx frontend/src/screens/Processos.tsx frontend/src/main.tsx frontend/tests/process-modal-contract.test.mjs
git commit -m "refactor: share process details modal"
```

---

