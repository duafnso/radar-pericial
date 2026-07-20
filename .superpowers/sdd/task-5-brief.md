### Task 5: Docker, Dados Reais E Regressão

**Files:**
- Modify: `tools/smoke_test.py`
- Modify: `docs/PENDENCIAS_PRODUCAO.md`
- Modify: `docs/DEPLOY_PRODUCAO.md`

**Interfaces:**
- Smoke check: authenticated `/api/processos/mapa/resumo`

- [ ] **Step 1: Extend smoke test**

Add:

```python
("mapa processos", "/api/processos/mapa/resumo?limit_cidades=200"),
```

Validate that the returned payload contains:

```python
{"total_processos", "total_municipios", "sem_localizacao", "items"}
```

- [ ] **Step 2: Run smoke test source checks**

```bash
python -m py_compile tools/smoke_test.py api/main.py database/db.py
```

Expected: PASS.

- [ ] **Step 3: Rebuild all application services**

```bash
docker compose up -d --build web worker beat
docker compose ps
```

Expected: web, worker and beat `Up`; web, db and redis healthy.

- [ ] **Step 4: Run complete backend suite against local code**

```bash
docker compose run --rm -T -v "${PWD}:/app" -w /app web \
  sh -c "pip install --user -r requirements-dev.txt >/tmp/pip-test.log && \
  RUN_POSTGIS_INTEGRATION=true APP_ENV=test python -m pytest"
```

Expected: all tests PASS, including PostGIS integration.

- [ ] **Step 5: Validate real map totals**

Authenticated response must match database evidence:

- `total_processos`: 702 for the current unfiltered database;
- `total_municipios`: 73;
- `sem_localizacao`: 27.

If collection data changes during implementation, compare endpoint results with
fresh SQL counts rather than forcing these historical numbers.

- [ ] **Step 6: Run authenticated smoke**

```bash
docker compose exec -T web sh -c \
  'RADAR_SMOKE_USER=admin RADAR_SMOKE_PASSWORD="$DEFAULT_ADMIN_PASSWORD" \
  python tools/smoke_test.py --base-url http://127.0.0.1:8000'
```

Expected: every check `[OK]`.

- [ ] **Step 7: Visual validation**

Validate `http://localhost:8000` at:

- 1440x900;
- 1024x768;
- 390x844.

Check:

- basemap is not blank;
- markers are visible and compact;
- counts match the endpoint;
- selection opens the correct city;
- detail and follow actions work;
- tile failure warning does not remove process data;
- no polygons or territorial limits appear;
- no text or controls overlap.

- [ ] **Step 8: Update documentation**

Record:

- Leaflet is bundled;
- map endpoint aggregates before limiting;
- provider tile URL is configurable;
- OSM development-only caveat;
- current real totals from the final validation.

- [ ] **Step 9: Final verification**

```bash
git diff --check
npm run frontend:test
npm run frontend:build
```

Expected: exit code 0.

- [ ] **Step 10: Commit**

```bash
git add tools/smoke_test.py docs/PENDENCIAS_PRODUCAO.md docs/DEPLOY_PRODUCAO.md
git commit -m "docs: document territorial map operations"
```
