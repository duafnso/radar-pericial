# Revisão Final Independente - Tarefa 4: Mapa Territorial

## Veredito

**APPROVED**

As correções finais resolvem os dois achados Important e fortalecem a cobertura dos dois pontos Minor da re-revisão. Não foram encontrados novos defeitos Critical ou Important. Restam duas observações Minor que não bloqueiam a aprovação.

## Escopo

Foram revisados o pacote `.superpowers/sdd/task-4-final-review.diff`, o relatório atualizado e a implementação atual de mapa, modal, modelo, tipos, estilos, API, banco e testes relacionados. `ApiClient`, `Processos.tsx`, scripts npm e dependências foram inspecionados para detectar regressões. Nenhum código foi editado.

## Critical

Nenhum achado Critical.

## Important

Nenhum achado Important.

## Minor

### [M1] Falha na segunda página pode exibir “página 2 de 1”

**Arquivo/linha:** `frontend/src/screens/MapScreen.tsx:313`

O ramo de erro agora limpa corretamente `processes` e `processTotal`, mas mantém `page`. Se a página 2 falhar, `processTotal=0` faz `pageCount` voltar a 1, enquanto `page` continua em 1; o rodapé das linhas 692-698 pode mostrar `0 processos · página 2 de 1`. As linhas e o total obsoletos não permanecem, e o usuário ainda pode voltar à página anterior, portanto a inconsistência é cosmética e não bloqueante.

### [M2] Corrida da listagem municipal continua sem teste específico

**Arquivo/linha:** `frontend/tests/map-screen.behavior.test.tsx:298`

O novo teste de resposta fora de ordem é comportamental e comprova o descarte de um resumo antigo por `summaryRequestRef`. A listagem municipal usa corretamente o cleanup `active=false`, mas ainda não há teste que troque cidade ou página e resolva a request antiga por último. Trata-se de lacuna de cobertura, não de falha observada na implementação.

## Correções Específicas

### Limpeza após falha

- `MapScreen.tsx:313-317` limpa linhas e total antes de exibir erro.
- O teste RTL carrega 11 processos, avança para uma resposta inválida e comprova a remoção da linha e do total anteriores.
- A cidade selecionada e a ação de retry são preservadas.

### Município exato ponta a ponta

- O mapa envia `municipio_exato=true` com `URLSearchParams`.
- FastAPI declara a flag booleana e encaminha `municipio_exato` separadamente.
- `Database.listar_processos` compara com igualdade após normalizar caixa, espaços externos e sequências de espaços, sem wildcard.
- Sem a flag, o caminho parcial continua usando `ILIKE '%valor%'`.
- Testes de API verificam os dois encaminhamentos; testes de banco verificam SQL e parâmetros sem wildcard no modo exato e com wildcard no modo parcial.
- O `Processos.tsx` não envia a flag e preserva sua pesquisa parcial existente.

### Resposta fora de ordem

- O teste usa duas promises controladas, altera a região, resolve a resposta atual primeiro e a antiga depois.
- O DOM mantém Sinop e não volta a exibir Cuiabá, comprovando o guard de sequência do resumo.

### ProcessModal

- Quatro testes RTL reais montam o componente e exercitam foco inicial, fechamento por Escape, restauração de foco, backdrop, clique interno, wrap de Tab/Shift+Tab e payload de acompanhamento.
- Os contratos textuais permanecem apenas como cobertura estrutural complementar.
- O tipo genérico e `followDisabled` continuam compatíveis com `MapProcess` e com o `Processo` usado pelo Radar.

## Regressões Revalidadas

- Lifecycle Leaflet mantém uma instância por montagem e cleanup de frame, tiles, layers, mapa e listeners.
- Guards assíncronos descartam resumos e listagens obsoletos.
- Filtros e paginação usam `URLSearchParams`, encoding correto, `limit=10`, offset e total da resposta.
- Seleção centraliza em coordenadas da API; `fitBounds` usa apenas coordenadas finitas.
- Chamadas continuam no `ApiClient` autenticado, sem `fetch` paralelo.
- Tooltip usa `textContent` e o painel/modal usam escaping React; sem via de XSS identificada.
- Datas civis continuam sem conversão UTC.
- Acompanhamento mantém guarda síncrona contra POST concorrente.
- Tiles mantêm configuração pareada, atribuição e avisos sem ocultar dados.
- Não há polígonos, GeoJSON territorial ou coordenadas municipais manuais.
- Marcadores preservam dimensão estável, compactação, nome acessível, faixa textual, teclado e contraste mínimo.
- Overlays permanecem empilhados e o CSS responsivo não sofreu alteração final.
- TypeScript strict, Vitest e lockfile permanecem consistentes.

## Verificações Executadas

- `npm run frontend:test`: **PASS**; 14 contratos Node e 26 testes Vitest/RTL.
- `npm run frontend:typecheck`: **PASS**; 0 erros.
- `npm run frontend:build`: **PASS**; Vite 6.4.3, 1.606 módulos; CSS 35,01 kB e JS 415,60 kB antes de gzip.
- `npm audit --audit-level=critical`: **PASS**; 0 vulnerabilidades.
- Suíte backend indicada em Docker: **PASS**; 51 testes em `test_api_testclient.py` e `test_database_methods.py`.
- `npm ls --depth=0`: **PASS**; árvore direta consistente.
- `git diff --check`: **PASS**; somente warnings preexistentes de LF/CRLF.

Warnings backend não bloqueantes: futura dependência `pyarrow` do pandas, atalho `app` depreciado no httpx/Starlette e APIs depreciadas de passlib/argon2.

## Riscos Residuais

- Não houve smoke autenticado com API e PostGIS reais.
- Os testes de `Database.listar_processos` validam a construção do SQL, mas não executam a expressão de normalização contra PostgreSQL real.
- Não houve validação visual em browser real nos viewports 1440x900, 1024x768 e 390x844.
- A normalização exata não remove diacríticos; o funcionamento depende da normalização municipal já aplicada aos dados persistidos.
- O resumo usa o score mais recente via `LATERAL`, enquanto a listagem mantém join direto; registros legados duplicados em `score_pericial` podem causar divergência até o schema impor unicidade.
- O payload do resumo permanece confiado por tipo estático, sem parser de runtime equivalente ao da listagem.