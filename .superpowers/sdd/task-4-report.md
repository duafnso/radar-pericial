# Relatório da Tarefa 4: Mapa Territorial

## Status

Concluída e commitada localmente, sem push. A rodada de revisão corrigiu os 8 achados Important e os 2 Minor.

## Implementação original

- Leaflet e CSS importados localmente, sem CDN, `unpkg` ou `CITY_COORDS`.
- Resumo municipal consumido de `/api/processos/mapa/resumo`; seleção consulta `/api/processos` com `limit=10`, offset paginado e filtros ativos.
- Uma instância Leaflet por montagem, um marcador por cidade, coordenadas vindas da API, sem polígonos ou limites territoriais.
- Painel municipal com seleção, centralização, paginação, `ProcessModal`, acompanhamento, feedback e navegação para alertas.
- Estados de loading, vazio, erro de API e falha de tiles preservam os dados úteis.
- Tile URL e atribuição configuráveis, com OSM apenas como fallback de desenvolvimento e atribuição visível.
- Layout responsivo para desktop, notebook e mobile, com dimensões estáveis e cleanup de mapa, layers e listeners.

## Correções da revisão

- **I1:** `parseProcessListResponse` valida `total`, `offset`, `limit`, `items` e cada processo. Payload inválido exibe erro/retry e preserva a cidade selecionada.
- **I2:** `formatCivilDate` formata somente `YYYY-MM-DD` como data civil, sem `Date`, UTC ou horário; lista e modal usam o helper.
- **I3:** cada `divIcon` recebe `role=button`, `tabindex=0`, `aria-label` com município, contagem e faixa, suporte a Enter/Espaço e remoção explícita do handler.
- **I4:** faixa textual foi adicionada ao tooltip e nome acessível. Os quatro verdes usados em marcadores, legenda e chips têm contraste calculado >= 4,5:1 com texto branco.
- **I5:** `followInFlightRef` faz guarda síncrona contra reentrada; ações da lista e modal ficam desabilitadas enquanto há POST em andamento.
- **I6:** loading, aviso de tiles e aviso de configuração usam uma única pilha de overlays com gap em todos os breakpoints.
- **I7:** adicionados Vitest, jsdom e Testing Library com mocks determinísticos de Leaflet e `ApiClient`; os contratos Node existentes foram preservados.
- **I8:** adicionados tipos React/ReactDOM, referência `vite/client`, tipos das variáveis de ambiente e script `frontend:typecheck`; strict está verde.
- **M1:** URL customizada só é aceita junto com atribuição customizada; configuração incompleta usa fallback OSM com aviso honesto.
- **M2:** a tela usa `MapProcess` estreito e valida os campos críticos no runtime sem casts amplos para o modelo final.

## Evidência TDD

### RED

- Os 12 testes puros iniciais falharam antes da criação dos validadores, formatador civil, resolvedor de tiles e paleta com contraste.
- Na primeira execução comportamental da tela, 6 testes falharam e 2 passaram. As falhas reproduziram I1, I2, I3, I5 e I6.
- Depois da implementação, 18 testes passaram e 2 expuseram uma expectativa incorreta de contagem de `setView`; corrigida a medição pelo baseline do mapa, a suíte ficou verde.

### GREEN final

```text
npm run frontend:test
```

- Contratos Node: 14 testes, 14 passaram.
- Vitest/RTL: 2 arquivos, 20 testes, 20 passaram.
- Cobertura comportamental: payload inválido/erro/retry, data civil, seleção e paginação, ARIA e teclado dos marcadores, cleanup, guarda de acompanhamento, modal desabilitado e falha de tiles sem ocultar dados.

## Typecheck, build e audit

```text
npm run frontend:typecheck
npm run frontend:build
npm audit --audit-level=critical
```

- TypeScript strict: PASS, 0 erros.
- Vite 6.4.3: PASS, 1.606 módulos transformados.
- Bundle: CSS 35,01 kB; JS 415,56 kB antes de gzip.
- Audit após atualizar Vitest para a linha corrigida 3.2.7: 0 vulnerabilidades.
- `git diff --cached --check`: PASS antes do commit.

## Commit da revisão

`b2e4b8e3bf77e7be0cbb99c10924e7a9ed964268` - `fix: harden territorial map interactions`

O commit contém somente 13 arquivos relacionados às correções: implementação/tipos do frontend, CSS, contratos e testes comportamentais, configuração Vitest e dependências. Nenhum arquivo de `.superpowers/sdd`, backend ou `interface/` foi incluído. Não houve push.

Commit original da Tarefa 4: `b0b1f6608ebb1e3518de628d2f942d2fea8782c9`.

## Auto-revisão

- Confirmada validação de runtime antes de substituir processos ou limpar seleção.
- Confirmada ausência de `Date` no caminho de data civil.
- Confirmados nomes acessíveis, equivalência de teclado e cleanup dos handlers dos marcadores.
- Confirmada faixa textual independente de cor e contraste mínimo por teste calculado.
- Confirmada guarda síncrona antes do primeiro `await` e bloqueio de ações concorrentes.
- Confirmada separação estrutural dos overlays.
- Confirmados typecheck, testes, build e audit após a última alteração.
- Confirmado commit seletivo; alterações anteriores em `.superpowers/sdd` e artefatos gerados em `interface/` permanecem fora do commit.

## Riscos residuais

- A validação visual autenticada em browser continua pendente para a Tarefa 5. jsdom e mocks provam os fluxos de componente, mas não substituem renderização real do Leaflet, tiles e CSS.
- Não foi executado smoke integrado com API/PostGIS autenticada; divergências entre o endpoint agregado e a listagem ainda dependem de dados reais.
- O fallback OSM depende de rede externa e deve permanecer restrito ao desenvolvimento; produção deve fornecer URL e atribuição em conjunto.

## Checklist visual pendente para a Tarefa 5

- [ ] 1440x900: mapa, marcadores, painel de 360 px, overlays e atribuição sem sobreposição.
- [ ] 1024x768: painel abaixo do mapa, loading e falha de tiles simultâneos sem colisão.
- [ ] 390x844: filtros, resumo, mapa, lista, paginação e botões sem corte ou overflow.
- [ ] Verificar foco visível e ativação por Enter/Espaço em marcadores reais do Leaflet.
- [ ] Confirmar tooltip com município, contagem, maior score e faixa textual.
- [ ] Confirmar contraste visual dos marcadores, chips e legenda nos tiles reais e no fundo neutro.
- [ ] Confirmar seleção, centralização, paginação, modal e acompanhamento com API autenticada.
- [ ] Simular tileerror mantendo marcadores, ranking e processos navegáveis.
- [ ] Confirmar atribuição customizada e aviso de configuração incompleta.
- [ ] Confirmar ausência de polígonos e limites territoriais.
## Rodada de re-revisão

### Correções

- **Important - dados obsoletos:** qualquer resposta nula ou shape paginado inválido agora limpa `processes` e `processTotal` no mesmo ramo que exibe o erro, preservando somente a cidade e o retry. O teste executa carga válida com 11 processos, avança a página para uma resposta inválida e comprova a remoção da linha e do total anteriores.
- **Important - município exato:** `/api/processos` aceita `municipio_exato=true`. O endpoint encaminha `municipio_exato` separadamente para `Database.listar_processos`; o banco compara município com `btrim`, espaços normalizados e `lower`, sem `%`. Sem a flag, o Radar continua usando `ILIKE` com `%valor%`.
- **Minor - corrida do resumo:** teste com duas promises resolve a requisição atual antes da antiga e comprova que `summaryRequestRef` impede a resposta obsoleta de substituir a cidade atual.
- **Minor - ProcessModal:** quatro testes RTL reais cobrem foco inicial e restauração, Escape, clique no backdrop versus interior do diálogo, wrap de Tab/Shift+Tab e payload da ação de acompanhamento. Os contratos Node foram preservados como cobertura complementar.

### Evidência RED/GREEN

RED frontend:

- `clears stale rows and total when a later page response is invalid`: falhou porque `STALE-PROCESS` permanecia no DOM.
- `selects, centers and requests the next ten-process page`: falhou porque a URL não continha `municipio_exato=true`.

RED backend no Docker:

- Os dois testes TestClient falharam porque o endpoint não chamava `listar_processos` e não encaminhava filtros separados.
- Os dois testes de banco falharam com ausência de `Database.listar_processos`.

GREEN final:

```text
npm run frontend:test
npm run frontend:typecheck
npm run frontend:build
npm audit --audit-level=critical
docker compose run --rm --no-deps --volume "${PWD}:/workspace" --workdir /workspace --user root --env PYTHONPATH=/workspace web sh -c "python -m pip install -q pytest pytest-asyncio httpx==0.27.2 && python -m pytest tests/test_api_testclient.py tests/test_database_methods.py -q"
```

- Contratos Node: 14/14.
- Vitest/RTL: 26/26 em 3 arquivos.
- Backend Docker: 51/51 nos dois arquivos de escopo.
- TypeScript strict: PASS.
- Build Vite: PASS, 1.606 módulos; CSS 35,01 kB e JS 415,60 kB antes de gzip.
- Audit: 0 vulnerabilidades.
- Warnings backend não bloqueantes e preexistentes: futura dependência pyarrow do pandas, atalho `app` depreciado no httpx/Starlette e APIs depreciadas de passlib/argon2.

### Commit da re-revisão

`64e50ffb6b9e58ab0bda198cfa19a4d752e2b25a` - `fix: align territorial map process results`

O commit contém somente 7 arquivos relacionados: `api/main.py`, `database/db.py`, `frontend/src/screens/MapScreen.tsx`, três arquivos de teste alterados e o novo teste RTL do modal. Nenhum arquivo de `interface/` ou `.superpowers/sdd` foi incluído. Não houve push nem reescrita dos commits anteriores.

### Riscos residuais após a re-revisão

- A comparação exata normaliza caixa, espaços externos e sequências de espaços; ela não remove diacríticos porque a extensão PostgreSQL `unaccent` não é requisito do schema. Os nomes persistidos já passam pelo pipeline de normalização municipal.
- Continua pendente para a Tarefa 5 o smoke autenticado com API/PostGIS real e a validação visual nos três viewports.
- Os warnings de dependências da suíte Python devem ser tratados em atualização coordenada de FastAPI/Starlette/httpx, sem alteração oportunista nesta tarefa.
