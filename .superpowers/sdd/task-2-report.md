# Task 2 Report: Leaflet Local E Modelo Cartografico

## Resumo

- Adicionado Leaflet 1.9.4 e `@types/leaflet` 1.9.21 com versoes fixas.
- Atualizado `frontend:test` para carregar TypeScript com `--experimental-strip-types`.
- Adicionados os tipos cartograficos e os helpers puros `buildMapSummaryParams` e `markerTone`.
- O modelo nao importa Leaflet.

## TDD

RED registrado antes da implementacao:

```text
not ok 2 - map model module exists
Expected values to be strictly equal:
false !== true
```

O erro confirmou que `frontend/src/map/model.ts` ainda nao existia. A implementacao minima foi adicionada em seguida.

## Comandos E Resultados

| Comando | Resultado |
| --- | --- |
| `npm run frontend:test` (RED) | Falhou como esperado pela ausencia do modulo. |
| `npm install leaflet@1.9.4` | Sucesso. |
| `npm install --save-dev @types/leaflet@1.9.21` | Sucesso. |
| `npm run frontend:test` | Sucesso: 4 testes passaram. |
| `npm run frontend:build` | Sucesso: Vite build e sync-build concluiram. |
| `git diff --check` | Sucesso, sem erros. |

## Commit

- SHA: `1db0db1`
- Mensagem: `build: bundle leaflet for territorial map`

## Arquivos No Commit

- `package.json`
- `package-lock.json`
- `frontend/src/types.ts`
- `frontend/src/map/model.ts`
- `frontend/tests/map-model.test.mjs`

## Riscos

- O Node emite `MODULE_TYPELESS_PACKAGE_JSON` ao importar o arquivo `.ts` como ESM durante os testes. O script exigido passa e a alteracao de `type: module` foi evitada por ter impacto global fora do escopo.

## Auto-Revisao

- Confirmado que `buildMapSummaryParams` sempre inclui `limit_cidades=200` e omite filtros vazios.
- Confirmado que a regiao global prevalece sobre a local.
- Confirmado o mapeamento deterministico de todas as faixas e fallback `low`.
- Confirmado que o indice antes do commit continha somente os cinco arquivos autorizados, sem `.superpowers/sdd`.
