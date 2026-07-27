# Reavaliacao Final Transversal - Mapa Territorial

Data: 2026-07-27

Intervalo revisado: `273e69f..22e71a3`

Escopo: codigo runtime, testes, Docker/Compose, documentacao e dependencias.
Os arquivos `.superpowers/sdd/*.diff`, screenshots e demais artefatos de
coordenacao foram ignorados como objeto de revisao, conforme solicitado.

## Veredito

**CHANGES_REQUIRED**

As correcoes de tiles Docker (I1 anterior) e de geometria PostGIS (I3 anterior)
estao resolvidas. A validacao runtime do resumo (I2 anterior) tambem esta
correta na fonte TypeScript e nas imagens Docker reconstruidas, mas o bundle
frontend versionado continua sendo o artefato anterior a essa correcao. Assim,
o checkout executado diretamente pelo FastAPI ainda serve o comportamento que
motivou I2, embora o caminho Docker recomendado produza o bundle correto.

## Achados Por Severidade

### Critical

Nenhum.

### Important

#### [I1] O bundle versionado nao contem a correcao runtime do resumo

**Arquivos/linhas:** `frontend/src/map/model.ts:151`,
`frontend/src/screens/MapScreen.tsx:95`,
`frontend/src/screens/MapScreen.tsx:154`, `index.html:6`,
`interface/templates/index.html:7`,
`interface/static/assets/index-Dshn0uJy.js:203`, `api/main.py:419-462`

O commit final alterou `frontend/src/map/model.ts`,
`frontend/src/screens/MapScreen.tsx` e `index.html`, mas nao regenerou nem
versionou `interface/templates/index.html` e `interface/static/assets`.
`git diff --name-status 7f10dfd..22e71a3 -- frontend/src index.html interface`
mostra somente os tres arquivos de fonte; os assets rastreados sao exatamente
os existentes em `7f10dfd`.

O bundle rastreado confirma o impacto: depois de buscar
`/api/processos/mapa/resumo`, ele copia diretamente
`total_processos`, `total_municipios`, `sem_localizacao` e `items` do payload e
depois executa `items.filter`. Ele nao chama `parseMapSummaryResponse` nem limpa
processos, paginacao e modal pela rotina nova. Um HTTP 200 com `items: null` ou
estrutura equivalente ainda pode derrubar a tela quando o FastAPI e iniciado
diretamente a partir do checkout.

O template rastreado aponta para `index-Dshn0uJy.js` e tambem nao contem o novo
favicon. Em contraste:

- o container default em execucao serve `index-B0ZfI_4i.js` e o favicon;
- o build Docker sentinela fresco gerou `index-D5G04doA.js`, incluiu o favicon
  e incorporou URL e atribuicao sentinela;
- ambos foram reconstruidos da fonte atual e, por isso, contem a correcao.

Portanto, suites de fonte, build Docker e smoke passam, mas nao provam que os
assets rastreados estao sincronizados. O CI executa `npm run frontend:build`,
mas nao falha se esse build deixar mudancas em `interface/templates` ou
`interface/static/assets`.

**Correcao necessaria:** regenerar os assets com a configuracao deterministica
de desenvolvimento, versionar o novo template, JS e favicon e remover o bundle
obsoleto. Adicionar uma verificacao de CI que execute o build e exija diff limpo
nesses caminhos, ou deixar de versionar os compilados e tornar o build
obrigatorio em todo caminho de execucao. Na arquitetura atual, a primeira opcao
e a mudanca de menor alcance.

### Minor

Nenhum achado adicional que bloqueie o mapa.

## Situacao Dos Tres Important Anteriores

### I1 anterior - configuracao de tiles no build Docker

**Resolvido.** O `Dockerfile` declara `ARG`/`ENV` antes do build Vite; Compose
development encaminha o par opcional e Compose production exige URL e
atribuicao. O build sentinela fresco encontrou ambos os valores no JS
compilado. Compose production passou com o par completo e falhou, como
esperado, com URL vazia.

### I2 anterior - validacao runtime do payload agregado

**Resolvido na fonte e no Docker; nao resolvido no artefato versionado.**
`parseMapSummaryResponse` valida raiz, totais, itens, coordenadas e contagens;
`MapScreen` consome `unknown` e limpa o estado dependente em falha. Os testes
cobrem payloads invalidos, estado vazio, limpeza e retry. O achado Important
desta reavaliacao impede considerar a correcao transversalmente concluida.

### I3 anterior - geometrias invalidas ou vazias

**Resolvido.** Itens e totais usam geometria nao nula, `ST_IsValid` e
`NOT ST_IsEmpty`; os demais registros entram em `sem_localizacao`. O teste
PostGIS adversarial passou. A base atual possui zero geometrias invalidas,
vazias, fora de SRID 4326 ou com ponto fora da faixa latitude/longitude.

## Contratos Confirmados

- **API/DB:** o resumo usa o score mais recente por processo, agrega antes de
  `limit_cidades`, aplica filtros nomeados e preserva contagens globais.
- **Dados reais:** endpoint sem filtro retornou `702 / 73 / 27`, 73 itens e
  soma municipal 702; SQL com o mesmo predicado retornou `702 / 73 / 27`.
- **Painel municipal:** usa municipio exato normalizado, pagina de 10 e preserva
  regiao, faixa e datas; a busca parcial do radar geral permanece compativel.
- **Autenticacao:** endpoints legado e agregado retornaram 401 sem Bearer. O
  fluxo autenticado de detalhes e acompanhamento continua protegido.
- **Endpoint legado:** chamada real autenticada retornou HTTP 200 com `total`,
  `items` e 120 registros no limite padrao.
- **Seguranca:** SQL usa clausulas fixas e parametros nomeados; dados de API sao
  renderizados por React ou `textContent`; nao foram encontrados segredos,
  coordenadas municipais manuais ou HTML de processo concatenado.
- **Leaflet:** codigo e CSS entram pelo bundle npm; nao ha CDN, GeoJSON,
  poligonos ou limites territoriais no novo mapa.
- **Tiles:** falha do basemap mantem marcadores e painel; atribuicao permanece
  visivel; producao exige provedor e atribuicao no build.
- **Lifecycle e acessibilidade:** uma instancia do mapa por montagem, cleanup de
  listeners/layers/map, descarte de respostas obsoletas, marcadores com nome e
  teclado, modal com trap/restauracao de foco e controles nativos.
- **Layout:** dimensoes e breakpoints estaveis constam no CSS e nos testes. A
  evidencia visual anterior cobre 1440x900, 1024x768 e 390x844 sem overflow.

## Evidencias Frescas

- `npm run frontend:test`: 14/14 contratos Node e 35/35 testes Vitest/RTL.
- `npm run frontend:typecheck`: passou (`tsc --noEmit`).
- Build Docker sentinela: passou; Vite compilou 1.606 modulos e gerou JS,
  CSS, favicon e template; URL e atribuicao sentinela foram encontradas no JS.
- `RUN_POSTGIS_INTEGRATION=true APP_ENV=test python -m pytest -q` em container:
  90 passed, 34 warnings.
- Smoke autenticado: 12/12 checks passaram, incluindo frontend, favicon, login,
  resumo do mapa e dependencias.
- PostGIS real: `702 / 73 / 27`; 73 itens no endpoint e soma agregada 702.
- Integridade municipal: `0 / 0 / 0 / 0` para geometrias invalidas, vazias,
  SRID diferente de 4326 e coordenadas fora da faixa.
- Endpoints sem token: legado 401 e agregado 401.
- Endpoint legado autenticado: HTTP 200, chaves `items` e `total`.
- Compose development: configuracao valida.
- Compose production: configuracao valida com sentinelas e falha esperada sem
  `VITE_MAP_TILE_URL`.
- `npm audit --audit-level=critical`: exit 0; uma vulnerabilidade `high`
  preexistente em PostCSS, nenhuma `critical`.
- `git diff --check` no escopo runtime/testes/docs: passou.
- As verificacoes nao alteraram arquivos rastreados.

Nao foi possivel acrescentar uma nova sessao visual pelo navegador integrado:
o processo do navegador falhou no ambiente Windows com erro 1058. Esta
limitacao nao invalida as suites comportamentais nem a evidencia visual anterior,
mas a homologacao humana continua pendente.

## Riscos Residuais

- O filtro inicial `data_inicio=2026-01-01` mostra atualmente 143 processos
  localizados em 54 municipios e 27 sem localizacao; `702 / 73 / 27` e o recorte
  historico sem filtro. Isso segue o plano implementado, mas deve permanecer
  explicito ao interpretar o criterio de cobertura inicial.
- Provedor comercial de tiles, atribuicao aprovada e homologacao humana nos
  tres viewports continuam pendentes antes do uso comercial.
- O build Docker sentinela e a fixture transacional do metodo PostGIS completo
  ainda nao fazem parte da automacao versionada.
- `npm audit` registra `GHSA-r28c-9q8g-f849` em PostCSS. A dependencia fica no
  estagio de build e o problema nao foi introduzido pelo commit final, mas deve
  ser atualizado separadamente.
- A suite backend mantem 34 warnings de deprecacao de pandas, passlib, httpx e
  argon2.
- No estado Docker observado, `web`, `db` e `redis` estavam saudaveis; `worker`
  e `beat` estavam parados desde uma execucao anterior. Isso nao indica regressao
  no diff do mapa, mas o smoke/readiness atual nao comprova disponibilidade de
  workers Celery em um deploy.

## Conclusao

O codigo-fonte e o caminho Docker satisfazem os contratos funcionais e os tres
ajustes tecnicos anteriores. O pacote transversal ainda nao esta pronto porque
o runtime frontend rastreado e servido diretamente pelo FastAPI ficou uma
geracao atras. Depois de sincronizar os assets e proteger essa sincronizacao no
CI, a reavaliacao pode mudar para **READY** sem nova mudanca funcional no mapa.
