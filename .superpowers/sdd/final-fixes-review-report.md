# Revisao Independente Das Correcoes Finais Do Mapa Territorial

Data: 2026-07-27

Intervalo revisado: `7f10dfd..22e71a3`

Commit revisado: `22e71a3 fix: harden territorial map contracts`

## Veredito

**APPROVED**

As correcoes resolvem os tres achados Important da revisao transversal e os
itens auxiliares solicitados. Nao encontrei regressao, vulnerabilidade nova,
quebra de compatibilidade ou mudanca fora do escopo que justifique bloquear o
commit.

## Achados Por Severidade

### Critical

Nenhum.

### Important

Nenhum.

### Minor

Nenhum.

## Contratos Confirmados

### I1 - Configuracao de tiles no build Docker

- `Dockerfile:5-14` declara `ARG` e `ENV` para URL e atribuicao antes de
  `npm run frontend:build`.
- `docker-compose.yml:12-14` encaminha o par e aceita ambos vazios no ambiente
  de desenvolvimento. Nesse caso, o bundle usa o fallback OpenStreetMap com
  atribuicao.
- `docker-compose.prod.yml:39-41` usa interpolacao `:?` para os dois valores.
  A configuracao de producao passa com o par completo e falha quando URL ou
  atribuicao esta vazia.
- Os valores `VITE_*` estao identificados como publicos em `.env.example`.
  `.env` nao e rastreado e tambem e excluido pelo `.dockerignore`. O estagio
  runtime nao herda os `ENV` do estagio frontend.
- `docs/DEPLOY_PRODUCAO.md` e `docs/PENDENCIAS_PRODUCAO.md` registram que a
  troca do provedor exige rebuild e que OSM e apenas fallback de desenvolvimento.
- Um build Docker sentinela real encontrou URL e atribuicao customizadas no
  JavaScript compilado. Um segundo build sem argumentos encontrou a URL OSM e
  nao encontrou as sentinelas.

### I2 - Parser runtime e limpeza de estado

- `frontend/src/map/model.ts:104-172` valida o objeto raiz, os tres totais,
  `items`, todos os campos de cada municipio, inteiros nao negativos,
  numeros finitos e limites validos de latitude/longitude.
- `null`, `items: null`, totais de tipo incorreto, coordenadas nao finitas,
  contagens negativas e municipio vazio sao rejeitados.
- `items: []` e aceito como estado vazio valido.
- `frontend/src/screens/MapScreen.tsx:95-167` consome a resposta como
  `unknown`, aplica o parser e, em falha, limpa resumo, selecao, processos,
  total do painel, pagina, modal, erro e loading dependentes antes de mostrar
  a acao de retry.
- Os testes comportamentais cobrem parser valido/vazio, payloads malformados,
  limpeza de dados antigos, fechamento do painel e retry. As protecoes ja
  existentes para respostas fora de ordem continuam passando.

### I3 - Predicado PostGIS localizavel

- `database/db.py:1731-1754` usa o mesmo contrato logico em itens e totais:
  geometria nao nula, `ST_IsValid` e `NOT ST_IsEmpty`.
- Geometria nula, invalida ou vazia entra em `sem_localizacao`; somente uma
  geometria localizavel chega a `ST_PointOnSurface` nos itens.
- O teste PostGIS isolado usa CTE sem alterar tabelas persistentes e cobre
  formas valida, invalida, vazia e ausente.
- Uma verificacao adicional desta revisao executou o metodo real
  `Database.resumo_mapa_processos` sobre tabelas temporarias dentro de uma
  transacao revertida. Resultado: 1 processo localizado, 1 municipio,
  3 sem localizacao e somente o item valido desenhavel.
- Na base atual, endpoint e SQL com o mesmo predicado retornaram
  `702 / 73 / 27`.

### Compatibilidade, favicon e documentacao

- `/api/processos/mapa` permanece registrado e sem alteracao no corpo do
  endpoint. A regressao HTTP autenticada passou e uma chamada real retornou
  HTTP 200 com as chaves legadas `total` e `items`.
- `index.html` referencia o SVG existente. O build Vite o emitiu como asset
  versionado sob `/static/assets/`, e o smoke obteve esse arquivo com HTTP 200.
- A documentacao visual agora distingue a execucao em Edge headless da
  homologacao humana ainda pendente.

### Seguranca, dependencias e escopo

- A varredura do diff nao encontrou chave, senha, token real ou material de
  chave privada. A unica ocorrencia de token e `token-viewer`, fixture de teste.
- Nao houve alteracao em `package.json`, lockfile ou requirements. Nenhuma
  dependencia foi adicionada pelo commit.
- O diff contem somente os 16 arquivos relacionados as correcoes, testes,
  favicon e documentacao. `git diff --check 7f10dfd..22e71a3` passou.
- Dados de API continuam renderizados por React ou `textContent`; SQL continua
  usando clausulas fixas e parametros nomeados.

## Verificacoes Frescas

- `npm run frontend:test`: 14/14 contratos Node e 35/35 testes Vitest/RTL.
- `npm run frontend:typecheck`: passou (`tsc --noEmit`).
- Build Docker `frontend-build` com sentinelas: passou; URL e atribuicao foram
  encontradas no bundle.
- Build Docker `frontend-build` sem argumentos: passou; fallback OSM presente
  e sentinelas ausentes.
- Compose desenvolvimento com par vazio: configuracao valida.
- Compose producao com par completo: configuracao valida.
- Compose producao sem URL: falha esperada.
- Compose producao sem atribuicao: falha esperada.
- `RUN_POSTGIS_INTEGRATION=true python -m pytest`: 90 passed, 34 warnings.
- Fixture PostGIS transacional do metodo real: `1 / 1 / 3`, um item valido.
- Smoke autenticado: todos os 12 checks passaram, incluindo mapa e favicon.
- Endpoint agregado e SQL real: `702 / 73 / 27` em ambos.
- Endpoint legado real autenticado: HTTP 200, contrato preservado.
- `npm audit --audit-level=critical`: exit 0, nenhuma vulnerabilidade critical.

## Riscos Residuais E Limitacoes

- O teste versionado de Docker e um contrato estatico; o build sentinela real
  foi executado nesta revisao, mas nao esta automatizado na suite. Mudancas
  futuras no pipeline ainda devem repetir essa verificacao de release.
- O teste PostGIS versionado combina assercoes do SQL do metodo com uma CTE
  isolada. A integracao do metodo completo com fixture temporaria foi validada
  nesta revisao, mas essa verificacao adicional tambem nao esta automatizada.
- `npm audit` registra uma vulnerabilidade high preexistente em `postcss`
  (`GHSA-r28c-9q8g-f849`). Ela nao foi introduzida nem ampliada pelo commit,
  mas deve ser tratada em atualizacao de dependencias separada.
- A suite backend emite 34 warnings de deprecacao de pandas, passlib, httpx e
  argon2. Nao afetam o veredito deste commit.
- Permanece pendente homologacao humana nos tres viewports e com o provedor
  comercial definitivo. Valores `VITE_*` sao publicos no bundle e nunca devem
  receber credenciais que precisem permanecer secretas.

## Conclusao

O commit `22e71a3` esta aprovado para integrar as correcoes finais do Mapa
Territorial. Os riscos restantes sao operacionais ou de automacao futura e nao
indicam defeito funcional no intervalo revisado.
