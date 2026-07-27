# Final Assets Report

Data: 2026-07-27

Commit final: `d426be89673f6027a9a95444703a9f9f0c301ced` (`fix: make frontend assets reproducible on Linux`)

Commit anterior preservado: `d2a9117106e839d3e3904dc9dc4446873ececc8e`.

## Causa Raiz

O hash dependia de duas fronteiras ambientais. O bundle compilado no host Windows divergia do Linux, e no Linux a ausencia das variaveis `VITE_MAP_TILE_URL`/`VITE_MAP_TILE_ATTRIBUTION` gerava `index-BeEPyqDJ.js`, enquanto o Dockerfile as define como strings vazias e gera `index-B0ZfI_4i.js`. A CI agora explicita os mesmos defaults vazios do Docker.

Node 20.20.2 foi avaliado no container Linux preferencial, mas `frontend:test:contracts` falhou porque esse runtime rejeita `--experimental-strip-types`. A configuracao final usa Node 22.23.1 e npm 10.9.8, que executam a suite atual e reproduzem o bundle Docker.

## Escopo Commitado

- `.github/workflows/ci.yml`: Node 22.23.1 fixo, verificacao de npm 10.9.8, defaults vazios de tiles e guarda de diff preservada.
- `Dockerfile`: imagem frontend fixada em `node:22.23.1-slim`.
- `tests/test_frontend_asset_sync_contract.py`: contrato de toolchain/defaults/ordem do diff, alem de bundle, favicon, parser e ausencia de unpkg.
- `interface/templates/index.html` e `interface/static/assets`: bundle Linux `index-B0ZfI_4i.js` versionado; hash Windows `index-BsQXFZTK.js` removido pelo sincronizador normal.

## Evidencias Linux

Ambiente: `node:22.23.1-slim`, Node `v22.23.1`, npm `10.9.8`, bind do repositorio em `/app` e volume Docker separado em `/app/node_modules`.

A mesma sequencia foi executada duas vezes com `VITE_MAP_TILE_URL=` e `VITE_MAP_TILE_ATTRIBUTION=`:

`npm ci && npm run frontend:test && npm run frontend:typecheck && npm run frontend:build`

Nas duas execucoes:

- 14/14 contratos Node passaram.
- 35/35 testes Vitest/RTL passaram.
- `tsc --noEmit` passou.
- Vite 6.4.3 transformou 1.606 modulos.
- foram gerados `index-B0ZfI_4i.js`, `index-CtHXTncZ.css` e `radar-pericial-logo-C70T_mVI.svg`.

O contrato Python foi executado em container Linux somente leitura e confirmou 2 checks: template/hash, favicon existente, token do fallback do parser, ausencia de `unpkg.com`, toolchain fixada, defaults vazios e guarda de diff apos build.

Apos staging e o segundo build, `git diff --exit-code -- interface/templates interface/static/assets` passou sem saida. Depois do commit, `git diff --exit-code HEAD -- interface/templates interface/static/assets` tambem passou sem saida.

## Exclusoes

`.superpowers/sdd`, `.env` e demais arquivos alheios ficaram fora do commit. Nenhum push foi realizado.