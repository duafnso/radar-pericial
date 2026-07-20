# Tarefa 5 - Relatorio de Implementacao e Verificacao

Data: 2026-07-20

## Escopo entregue

- `tools/smoke_test.py`: inclui a verificacao autenticada de `/api/processos/mapa/resumo?limit_cidades=200` e exige `total_processos`, `total_municipios`, `sem_localizacao` e `items`.
- `tests/test_smoke_test.py`: regressao para payload agregado incompleto.
- `frontend/src/screens/MapScreen.tsx`: o rodape normaliza apenas a pagina exibida apos falha, sem disparar uma nova request.
- `frontend/tests/map-screen.behavior.test.tsx`: cobre o rodape apos falha e resposta municipal fora de ordem.
- `docs/PENDENCIAS_PRODUCAO.md` e `docs/DEPLOY_PRODUCAO.md`: operacao do endpoint, tiles, atribuicao e checklist visual.

## TDD

- M1: o novo teste falhou ao procurar `pagina 1 de 1` depois de uma resposta invalida da pagina 2. A primeira hipotese com `setPage(0)` disparou nova busca e removeu o erro; a correcao final usa `displayedPage = min(page, pageCount - 1)`. A suite do MapScreen passou com 11 testes.
- Smoke: a prova de mutacao que removeu a validacao agregada falhou como esperado: `Failed: DID NOT RAISE <class 'AssertionError'>`. A validacao foi restaurada; a suite backend posterior passou.
- M2: o teste de resposta municipal fora de ordem foi adicionado. Ele passa com o cleanup `active` preexistente e confirma que `CUIABA-STALE` nao substitui `SINOP-CURRENT`.

## Comandos e resultados

- `docker compose up -d --build web worker beat`: sucesso; executado novamente apos a correcao final.
- `docker compose ps`: `web` saudavel; `db` e `redis` saudaveis; `worker` e `beat` ativos.
- `docker compose exec -T web python -m py_compile tools/smoke_test.py api/main.py database/db.py`: sucesso.
- `docker compose run --rm -T -v "${PWD}:/app" -w /app web sh -c "pip install --user -r requirements-dev.txt >/tmp/pip-test.log && RUN_POSTGIS_INTEGRATION=true APP_ENV=test python -m pytest"`: `85 passed, 33 warnings in 2.85s`.
- `npm run frontend:test`: 14 contratos Node e 27 testes Vitest/RTL passaram.
- `npm run frontend:typecheck`: sucesso, sem erros.
- `npm run frontend:build`: sucesso; Vite 6.4.3 compilou 1.606 modulos; JS final 415,64 kB e CSS 35,01 kB antes de gzip.
- `npm audit --audit-level=critical`: `found 0 vulnerabilities`.
- `docker compose exec -T web ... python tools/smoke_test.py --base-url http://127.0.0.1:8000`: todos os checks `[OK]`, inclusive `mapa processos`.
- Assets compilados: bundle local de Leaflet presente em `interface/static/assets/index-Dshn0uJy.js`; nenhuma ocorrencia de `unpkg.com` em templates ou assets.
- `git diff --check`: sucesso; somente warnings preexistentes de conversao LF/CRLF.

## Totais reais

| Fonte | Processos localizados | Municipios | Sem localizacao |
| --- | ---: | ---: | ---: |
| Endpoint autenticado `/api/processos/mapa/resumo` | 702 | 73 | 27 |
| SQL PostGIS | 702 | 73 | 27 |

O SQL executado contou processos com `municipios_mt.geometry IS NOT NULL`, municipios distintos por `m.nome` e processos sem geometria. Os totais correspondem; numeros nao foram forcados.

## Logs e riscos

- Os ultimos logs de `web` mostraram requests 200 e nao expuseram senha, token ou variaveis secretas.
- O ambiente local registrou `SESSION_TOKEN_PEPPER/SECRET_KEY ausente; usando pepper de desenvolvimento`; isto continua sendo risco de producao e ja esta coberto pelas pendencias de configuracao.
- Warnings nao bloqueantes: futura dependencia pyarrow do pandas e APIs depreciadas de passlib/argon2 e httpx/Starlette.

## Checklist visual

- Pendente. O navegador integrado falhou antes de abrir uma aba com Windows 1058 (`CreateProcessWithLogonW failed: 1058`).
- Nao foi declarada validacao visual concluida para 1440x900, 1024x768 ou 390x844.
- Permanecem para navegador real: basemap, marcadores, contadores, selecao, detalhes, acompanhamento, falha de tiles, ausencia de limites territoriais e ausencia de sobreposicoes.
