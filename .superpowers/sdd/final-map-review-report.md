# Revisao Final Transversal - Mapa Territorial

Data: 2026-07-20

Intervalo revisado: `273e69f..854541c` (nove commits de implementacao,
de `60d9d9d` a `854541c`; `273e69f` e o commit-base do plano).

## Veredito

**CHANGES_REQUIRED**

O fluxo atual funciona com os dados presentes e as evidencias funcionais sao
fortes, mas tres achados Important ainda violam contratos explicitos da
especificacao: configuracao do provedor de tiles no build Docker, validacao
runtime do payload agregado e exclusao de geometrias PostGIS invalidas/vazias.

## Critical

Nenhum.

## Important

### [I1] A imagem Docker nao recebe a configuracao do provedor de tiles

**Arquivos/linhas:** `Dockerfile:1-9`, `docker-compose.yml:8-12`,
`docker-compose.prod.yml:35-38`, `frontend/src/screens/MapScreen.tsx:52-55`,
`docs/DEPLOY_PRODUCAO.md:231-240`

`MapScreen` le `VITE_MAP_TILE_URL` e `VITE_MAP_TILE_ATTRIBUTION`, que sao
substituidas pelo Vite em tempo de build. Entretanto:

- o `Dockerfile` nao declara `ARG`/`ENV` para essas variaveis antes de
  `npm run frontend:build`;
- os Compose nao possuem `build.args`;
- `.env` e excluido do contexto e, de todo modo, nao e copiado para o estagio
  frontend;
- variaveis configuradas no `environment` do container final chegam tarde
  demais para alterar o bundle.

O bundle local inspecionado contem o fallback
`https://tile.openstreetmap.org/{z}/{x}/{y}.png`. Assim, o caminho de deploy
documentado nao consegue produzir uma imagem com o provedor comercial exigido,
apesar de a documentacao afirmar que as variaveis de build bastam.

Os testes validam apenas que os nomes das variaveis existem no fonte e que
`resolveTileConfig` escolhe o par informado. Nao ha teste do build Docker com
valores customizados.

**Correcao necessaria:** encaminhar URL e atribuicao como argumentos do build
Docker (e documentar que trocar o provedor exige rebuild), ou adotar
configuracao runtime deliberada e segura.

### [I2] Resposta agregada malformada pode derrubar a tela em vez de entrar no estado de erro

**Arquivos/linhas:** `frontend/src/screens/MapScreen.tsx:137-155`,
`frontend/src/screens/MapScreen.tsx:196`, `frontend/src/screens/MapScreen.tsx:440`,
`frontend/src/map/model.ts:76`, `tools/smoke_test.py:112-119`

O resumo e solicitado diretamente como `MapSummaryResponse` e somente `null` e
rejeitado. Campos e itens sao copiados sem validacao runtime. Um HTTP 200 com
`items: null`, `items` ausente ou contadores de tipo incorreto alcanca
`summary.items.filter`/`summary.items.length` e pode causar erro React, sem
limpar a tela de forma controlada nem oferecer retry.

A listagem municipal ja usa `parseProcessListResponse(unknown)`, mas nao existe
parser equivalente para `MapSummaryResponse`. O smoke verifica apenas que as
quatro chaves existem; nao valida tipos ou itens.

Os 41 testes frontend cobrem listagem municipal invalida, corridas, follow,
tiles e cleanup, mas nao cobrem resumo nulo, resumo malformado ou estado vazio
do resumo.

**Correcao necessaria:** adicionar parser runtime do resumo, tratar payload
invalido como falha da API e incluir testes comportamentais para `null`,
estrutura malformada e `items: []`.

### [I3] Geometria nao nula e tratada como geometria valida/localizavel

**Arquivos/linhas:** `database/db.py:1711-1734`,
`database/db.py:1737-1752`, `tests/test_database_methods.py:413-430`

O contrato exige desenhar somente municipios com geometria valida e contabilizar
os demais como sem localizacao. O SQL usa apenas `geometry IS NOT NULL`:

- nao exclui `NOT ST_IsValid(geometry)`;
- nao exclui `ST_IsEmpty(geometry)`;
- contabiliza esses casos como localizados nos totais;
- tenta aplicar `ST_PointOnSurface` a eles na consulta de itens.

A base atual nao manifesta o defeito: a verificacao fresca encontrou 141
geometrias municipais, zero nulas, zero invalidas e zero vazias. Isso nao
substitui o contrato no ponto de leitura, pois nao ha constraint de banco que
garanta a condicao para toda escrita futura.

Os testes de banco verificam `ST_PointOnSurface` e a presenca do filtro nao
nulo, mas nao exigem nem executam o predicado de validade/vazio em PostGIS.

**Correcao necessaria:** usar um predicado localizavel consistente nas consultas
de itens e totais, por exemplo geometria nao nula, valida e nao vazia, e cobrir
os casos invalido/vazio em teste PostGIS.

## Minor

### [M1] `git diff --check` falha no intervalo completo

`git diff --check 273e69f..854541c` retorna quatro erros `new blank line at EOF`
em `task-1-brief.md` ate `task-4-brief.md`. Nao afeta runtime, mas contradiz a
evidencia anterior de diff limpo para o intervalo transversal.

### [M2] Compatibilidade do endpoint legado nao possui regressao HTTP dedicada

`GET /api/processos/mapa` permanece no codigo e seu corpo nao foi alterado no
intervalo; a verificacao local sem token retornou 401, assim como o endpoint
novo. Entretanto, nao existe teste que proteja o contrato 200/payload do
endpoint legado.

### [M3] Documentacao visual usa formulacao imprecisa

`docs/PENDENCIAS_PRODUCAO.md:171-176` afirma que nenhum navegador real estava
disponivel, enquanto os artefatos registram Edge headless. A pendencia correta
e homologacao humana em navegador e com o provedor comercial definitivo.

## Contratos Confirmados

- **Endpoint legado:** `/api/processos/mapa` permanece registrado e sem mudanca
  funcional no diff; sem autenticacao responde 401.
- **Autenticacao:** `/api/processos/mapa/resumo` tambem responde 401 sem Bearer;
  o smoke obtem token no login e o reutiliza nas chamadas protegidas.
- **Filtro exato e parcial:** o mapa envia `municipio_exato=true`; a API encaminha
  chave separada; o banco usa igualdade normalizada sem wildcard. Sem a flag,
  permanece `ILIKE '%valor%'`. No PostGIS atual, `Vera` retornou 4 resultados
  exatos e 8 parciais.
- **Sem limites territoriais:** `MapScreen` nao carrega GeoJSON/poligonos. Os 54
  `path.leaflet-interactive` da evidencia sao os `CircleMarker` municipais.
- **Sem coordenadas municipais manuais:** nao ha `CITY_COORDS` ou tabela fixa;
  as coordenadas dos itens vem de `municipios_mt.geometry` por
  `ST_PointOnSurface`. O unico valor fixo e o centro estadual inicial do mapa.
- **Sem Leaflet por CDN:** Leaflet 1.9.4 e CSS entram no bundle npm; nao ha
  `unpkg`, `cdnjs` ou `jsdelivr` no runtime.
- **Seguranca de renderizacao:** tooltip usa `textContent`; painel e modal usam
  escaping React. SQL usa clausulas fixas e parametros nomeados. A varredura do
  diff nao encontrou credenciais ou chaves privadas.
- **Lifecycle Leaflet:** uma instancia por montagem, listeners/layers/tile/map
  removidos no cleanup, requests obsoletas descartadas e coordenadas nao finitas
  excluidas de bounds/marcadores.
- **Acessibilidade e responsividade:** marcadores possuem nome acessivel e
  ativacao por Enter/Espaco; modal possui trap/restauracao de foco; painel e
  controles usam elementos nativos. A evidencia registra ausencia de overflow
  em 1440, 1024 e 390 px.

## Evidencias

### Verificacoes frescas desta revisao

- `npm run frontend:test`: 14 contratos Node + 27 testes Vitest/RTL, todos
  passando.
- `npm run frontend:typecheck`: passou em modo `tsc --noEmit`.
- Smoke autenticado: 12/12 checks `[OK]`, incluindo login, asset e mapa.
- Docker: `web`, `db` e `redis` saudaveis; `worker` e `beat` ativos.
- PostGIS atual: 702 processos localizados, 73 municipios e 27 sem localizacao.
- Integridade atual: zero geometrias invalidas/vazias, zero nomes municipais
  normalizados duplicados e zero processos com scores duplicados.
- Endpoints legado e agregado sem token: ambos HTTP 401.
- Varredura de CDN, coordenadas manuais, poligonos territoriais e segredos:
  nenhuma ocorrencia de runtime relevante.
- `git diff --check 273e69f..854541c`: falhou apenas nos quatro EOFs descritos
  em M1.

### Evidencias consolidadas fornecidas

- Suite backend completa com PostGIS habilitado: 85 testes passando.
- Frontend strict/build/audit: typecheck e build concluido; audit critical com
  zero vulnerabilidades.
- API e metodo PostGIS: 702/73/27, com 73 itens sem limite truncado.
- Edge headless: 54 pins do recorte padrao 2026+, todos 26x26; 12 tiles;
  painel real com 10 processos; nenhum overflow em 1440/1024/390.
- Console: unico 404 associado a `favicon.ico`; nenhuma `pageerror`.

## Cobertura E Riscos Residuais

Os testes cobrem os principais fluxos felizes e varias falhas relevantes:
autenticacao do resumo, limites HTTP, filtros, SQL parametrizado, agregacao antes
do limite, selecao/paginacao, resposta fora de ordem, acompanhamento, modal,
falha de tiles e cleanup Leaflet.

Permanecem, alem dos achados:

- homologacao humana e escolha do provedor comercial de tiles;
- suite PostGIS automatizada valida schema, enquanto o SQL completo do mapa e
  exercitado pelo smoke/validacao local, nao por fixture de geometrias adversas;
- `score_pericial` nao possui constraint unica por processo, embora a base atual
  tenha zero duplicatas e o resumo selecione a linha mais recente;
- a leitura de 54 pins e coerente com o filtro operacional padrao `2026-01-01`,
  nao com uma consulta historica sem filtro;
- 33 warnings nao bloqueantes de dependencias Python/depreciacoes;
- `favicon.ico` ausente continua gerando o unico 404 observado.

O veredito pode mudar para **READY** depois de corrigir I1-I3, adicionar as
regressoes correspondentes e repetir build Docker, suites e smoke.
