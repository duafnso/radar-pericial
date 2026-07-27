# Re-revisao Independente Final - Sincronizacao de Assets

Data: 2026-07-27

Intervalo revisado: `d2a9117..d426be8`

Commit final: `d426be89673f6027a9a95444703a9f9f0c301ced`

## Veredito

**READY**

O achado Important anterior foi resolvido. A toolchain Linux e os defaults de
build agora estao alinhados entre CI e Docker, dois builds independentes sem
cache produziram os mesmos arquivos byte a byte, e esses arquivos coincidem
com os assets rastreados. Considerando as correcoes funcionais, de seguranca e
de integracao anteriores ja aprovadas, o estado transversal completo esta
pronto.

## Achados

Nenhum achado Critical, Important ou Minor no intervalo reavaliado.

## Confirmacoes

### Toolchain e defaults

- O Dockerfile fixa o estagio frontend em `node:22.23.1-slim`.
- A CI fixa `node-version: "22.23.1"` e verifica explicitamente Node
  `v22.23.1` e npm `10.9.8` antes da instalacao.
- O ambiente Linux executado confirmou Node `v22.23.1` e npm `10.9.8`.
- A CI define `VITE_MAP_TILE_URL` e `VITE_MAP_TILE_ATTRIBUTION` como strings
  vazias no job frontend. Isso equivale aos `ARG` nao informados promovidos a
  `ENV` vazios pelo Dockerfile.

### Reprodutibilidade e hashes

Dois builds Linux independentes foram executados com `--no-cache`, usando o
estagio `frontend-build` do Dockerfile. Ambos transformaram 1.606 modulos com
Vite 6.4.3 e geraram exatamente:

- `index-B0ZfI_4i.js`;
- `index-CtHXTncZ.css`;
- `radar-pericial-logo-C70T_mVI.svg`.

Os SHA-256 dos dois builds e do checkout coincidem:

- template: `ad2b7a4c697d572bd01f54ed7b433f4b89755d12098aee6c54cc99f1f68846ab`;
- JavaScript: `ae3c04a05777ee6c1e805751e833985203e6511987dff7f5ed856d563478afac`;
- CSS: `63f6fe96dcb562b89fb8fac019ce8eb5d7272bb2ca79773217a8cbed6b36e324`;
- favicon: `6ec24368bb77fd4a14962cbe3df6bffd92cbebb88b5f4d7fab637525339b4920`.

O template rastreado aponta somente para esses arquivos existentes. O bundle
Windows `index-BsQXFZTK.js` e o bundle ainda mais antigo
`index-Dshn0uJy.js` nao permanecem no diretorio de assets.

### CI e contrato

- A guarda de CI permanece depois do build e usa os paths corretos:
  `git diff --exit-code -- interface/templates interface/static/assets`.
- A mesma guarda contra `HEAD` passou sem saida no checkout reavaliado.
- Os dois contratos Python atualizados passaram em Linux, com o checkout
  montado somente leitura: `2 passed`.
- O contrato verifica bundle e favicon referenciados, fallback do parser,
  ausencia de `unpkg.com`, versoes de Node/npm, defaults Vite e ordem
  build-antes-do-diff.
- `frontend:test` passou no ambiente fixado: 14/14 contratos Node e 35/35
  testes Vitest/RTL.
- `frontend:typecheck` passou (`tsc --noEmit`).
- `git diff --check d2a9117 d426be8` passou.

### Seguranca e regressao

- O bundle rastreado preserva o parser/fallback controlado, o favicon e a
  ausencia de `unpkg.com`.
- Nenhum segredo foi introduzido. Os valores de CI sao placeholders de teste
  explicitamente nao produtivos; credenciais de smoke continuam vindo de
  `secrets.*`.
- O diff nao altera comportamento funcional de mapa, API, banco ou
  autenticacao. As correcoes transversais anteriores permanecem intactas.

## Riscos Residuais

- A imagem Node esta fixada por tag de versao, nao por digest. O build observado
  resolveu a tag para um digest especifico e foi reproduzivel, mas pin por
  digest reduziria ainda mais risco de supply chain; nao e bloqueante.
- Permanece a vulnerabilidade `high` preexistente em PostCSS, restrita ao
  estagio de build e nao introduzida por este commit. Deve ser atualizada em
  trabalho separado.
- Provedor comercial de tiles, atribuicao final e homologacao visual humana
  continuam itens operacionais antes do uso comercial, sem invalidar os
  contratos tecnicos aprovados.
- Warnings de depreciacao do backend e a verificacao operacional de workers
  Celery permanecem divida tecnica/deploy previamente registrada, sem
  regressao neste intervalo.

## Parecer Transversal

O commit `d426be8` fecha a unica pendencia da revisao anterior: os assets agora
sao reproduziveis no mesmo ambiente Linux usado pela CI e pelo Docker, o hash
rastreado e o hash gerado coincidem, e a guarda impede nova defasagem. Somado
as correcoes anteriores ja aprovadas, o pacote transversal final e **READY**.

