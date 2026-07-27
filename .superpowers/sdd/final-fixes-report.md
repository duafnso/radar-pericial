# Final Map Fixes Report

Date: 2026-07-27
Base HEAD: 7f10dfd

## Implemented

- I1: Docker passes public Vite tile URL and attribution through build args before the frontend build. Development permits the empty fallback pair; production requires both values. Documentation states that provider changes require rebuilding the image.
- I2: parseMapSummaryResponse validates the aggregate response at runtime. It rejects malformed structures, non-finite or negative numeric values, malformed city items, and null items. MapScreen clears stale summary, selection, panel, pagination, and modal state before showing retry.
- I3: Map aggregation only treats a municipality as localizable when geometry is non-null, valid, and non-empty. The same predicate is used for items and totals; invalid or empty geometry contributes to sem_localizacao.
- M2: authenticated HTTP regression covers GET /api/processos/mapa.
- M3: production notes now record Edge headless execution and retain human browser plus commercial-provider approval as pending.
- Favicon: the source index references the existing SVG logo; Vite emits it as a static asset.

## Evidence

- Frontend: 49 tests passed (14 Node contracts and 35 Vitest/RTL behaviors).
- Typecheck: npm run frontend:typecheck passed.
- Vite build: passed; emitted the logo SVG and compiled favicon reference.
- Audit: npm audit --audit-level=critical completed with zero critical vulnerabilities; npm reports one existing high severity PostCSS advisory.
- Docker: default web image build passed and its bundle contains the OpenStreetMap fallback. Sentinel image build passed and its bundle contains both sentinel tile URL and attribution.
- Compose: development configuration passed. Production configuration passed when both tile values were supplied and failed as expected when VITE_MAP_TILE_URL was omitted.
- Backend: 90 pytest tests passed with RUN_POSTGIS_INTEGRATION=true, including the isolated invalid/empty PostGIS geometry case.
- Smoke: authenticated smoke passed, including the map summary endpoint and favicon asset.
- API/SQL comparison: both returned 702 localized processes, 73 municipalities, and 27 without location.

## Residual Operational Work

- Human homologation remains required in a real browser at desktop, notebook, and mobile sizes.
- A commercial tile provider and its approved attribution remain required before commercial deployment.
