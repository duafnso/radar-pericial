# Task 3 Report: Reuse Process Details and Follow-up

## Status

Completed and committed.

## TDD Evidence

### RED

Created `frontend/tests/process-modal-contract.test.mjs` before production changes.

Command:

```text
npm run frontend:test
```

Result: failed as expected in `process modal exists and is shared by radar` because `frontend/src/components/ProcessModal.tsx` did not exist (`false !== true`). The remaining four existing frontend tests passed.

### GREEN

Extracted the existing detail modal into `frontend/src/components/ProcessModal.tsx` and imported it from `frontend/src/screens/Processos.tsx`.

The modal retains the existing `processo`, `close`, and `follow` contract. `follow` remains implemented in `Processos`, so the extraction does not add or duplicate API calls. The dialog retains backdrop close, labelled close control, `role="dialog"`, and `aria-modal`; it now also references its title with `aria-labelledby`.

`frontend/src/main.tsx` passes `navigate` and `notify` through a local compatibility component type. This prepares the Task 4 contract without changing the current `MapScreen` implementation or its current declared props.

## Verification

Final commands:

```text
npm run frontend:test
npm run frontend:build
git diff --check
git diff --cached --name-only
git diff --cached --check
```

Results:

- `npm run frontend:test`: 5 tests passed, 0 failed.
- `npm run frontend:build`: completed successfully with Vite production output.
- `git diff --check`: passed.
- Cached file list contained only the four task files.
- The frontend test command emits the pre-existing Node `MODULE_TYPELESS_PACKAGE_JSON` warning for `frontend/src/map/model.ts`; it does not fail the suite.

## Files Committed

- `frontend/src/components/ProcessModal.tsx`
- `frontend/src/screens/Processos.tsx`
- `frontend/src/main.tsx`
- `frontend/tests/process-modal-contract.test.mjs`

## Commit

`fe97c1526ef70ea9ae8aad009e05aaea98fad27e` - `refactor: share process details modal`

## Auto-review

Reviewed the staged diff before commit.

- The original modal body and follow action were moved rather than reimplemented.
- `Processos` owns the sole `POST /api/processos/:id/acompanhar` call, so no duplicate API call was introduced.
- The component uses the existing `Processo` type and formatting helpers.
- The map compatibility cast is local to `main.tsx`; no Task 4 map behavior was implemented.
- Only the four owned implementation/test files were staged and committed.

## Risks

- The current `MapScreen` does not yet declare or consume `navigate` and `notify`; Task 4 must replace the compatibility typing with its actual prop contract when it updates that screen.
- Build synchronization changes generated interface artifacts in the working tree; those files were deliberately excluded from the task commit.
## Review Remediation

Resolved both P2 findings from `task-3-review-report.md`.

### Modal keyboard behavior

- Captures `document.activeElement` when the modal mounts.
- Moves focus to the first focusable modal control, with the dialog as fallback.
- Closes on Escape.
- Wraps Tab and Shift+Tab within the modal controls.
- Removes the `keydown` listener during cleanup.
- Restores focus to the previously active element when it is still connected.
- Preserves backdrop closure, both close buttons, and the follow action.

### Contract coverage

Expanded `frontend/tests/process-modal-contract.test.mjs` to protect:

- `Processos` renders `ProcessModal` with `processo`, `close`, and `follow`.
- The three visible close paths invoke `close`.
- The follow button invokes `follow(processo)`.
- Escape, focus movement, Tab/Shift+Tab trapping, listener cleanup, and focus restoration are present.
- `main.tsx` passes `navigate` and `notify` to the map screen compatibility component.

### TDD evidence

RED: `npm run frontend:test` passed 7 contracts and failed the new keyboard-focus contract because `document.activeElement` was absent from `ProcessModal.tsx`.

GREEN: after implementation and correcting an initially over-specific Tab assertion, `npm run frontend:test` passed all 8 contracts.

### Final verification

- `npm run frontend:test`: 8 passed, 0 failed.
- `npm run frontend:build`: passed; Vite transformed 1,601 modules.
- `git diff --check`: passed.
- Existing `MODULE_TYPELESS_PACKAGE_JSON` warning for `frontend/src/map/model.ts` remains non-failing.

### Review fix commit

`f1ffb8531eca7b1fa788f3a633a93f178ca27482` - `fix: trap focus in process modal`

Files committed:

- `frontend/src/components/ProcessModal.tsx`
- `frontend/tests/process-modal-contract.test.mjs`

No `.superpowers/sdd` file, generated interface asset, or third-party change was included in the commit.
