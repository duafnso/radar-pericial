# Tarefa 5 - Relatorio de Revisao Tecnica Independente

Data: 2026-07-20

Commit revisado: `854541cc938dc539765847b63ef1dca4c5f9ae45`
Base: `64e50ffb6b9e58ab0bda198cfa19a4d752e2b25a`

## Veredito

APPROVED

O commit atende ao brief da Tarefa 5. Nao foram encontrados defeitos funcionais
ou exposicoes de credenciais que justifiquem solicitar alteracoes.

## Achados por severidade

### Critica

Nenhum.

### Alta

Nenhum.

### Media

Nenhum.

### Baixa

- O erro de console 404 da evidencia Edge e consistente com `GET /favicon.ico`
  nos logs do `web`. Nao ha indicio de falha de tile, asset do bundle ou API.
  E um problema cosmetico e nao bloqueia o mapa.

## Revisao dos seis arquivos

- `tools/smoke_test.py`: o smoke usa o token obtido no login para consultar
  `/api/processos/mapa/resumo?limit_cidades=200`, exige HTTP 200, payload
  objeto e presenca de `total_processos`, `total_municipios`,
  `sem_localizacao` e `items`.
- `tests/test_smoke_test.py`: o teste negativo percorre health, frontend,
  login e checks autenticados, confirma o token no endpoint do mapa e fornece
  payload agregado incompleto. A ausencia das chaves faz `run()` falhar em
  `mapa processos`; sem a validacao nova, o `pytest.raises` falharia.
- `frontend/src/screens/MapScreen.tsx`: `displayedPage` limita somente a pagina
  apresentada ao intervalo valido depois de payload invalido. O estado `page`
  nao e alterado, portanto o fix nao dispara uma segunda requisicao nem apaga
  prematuramente o erro.
- `frontend/tests/map-screen.behavior.test.tsx`: a regressao do rodape confirma
  limpeza de linhas e total antigos e exibe `pagina 1 de 1`. O teste de corrida
  troca Cuiaba por Sinop, resolve Sinop primeiro e Cuiaba depois, e confirma que
  `CUIABA-STALE` nao substitui `SINOP-CURRENT`. Isso exercita o cleanup
  `active` do efeito municipal.
- `docs/DEPLOY_PRODUCAO.md`: documenta Leaflet/CSS no bundle local, agregacao
  antes do limite, quatro campos do contrato, configuracao de URL e atribuicao
  de tiles, ressalva de OSM apenas para desenvolvimento, operacao do smoke e
  totais validados.
- `docs/PENDENCIAS_PRODUCAO.md`: registra os itens concluidos, totais reais e
  pendencias comerciais de provedor/atribuicao sem declarar homologacao humana
  concluida.

## Verificacoes executadas

- `git diff --check 64e50ff..854541c`: passou.
- `docker compose ps`: `web`, `db` e `redis` saudaveis; `worker` e `beat`
  ativos.
- `python -m py_compile tools/smoke_test.py api/main.py database/db.py` no
  container `web`: passou. O host nao possui interpretador Python local.
- Suite backend completa em container efemero com checkout montado e
  `RUN_POSTGIS_INTEGRATION=true`: `85 passed, 33 warnings`.
- `npm run frontend:test`: 14 contratos Node e 27 testes Vitest/RTL passaram,
  incluindo 11 testes de `MapScreen`.
- `npm run frontend:typecheck`: passou.
- Build Vite em diretorio temporario, sem alterar assets do worktree:
  1.606 modulos transformados; build concluido.
- Smoke autenticado contra `web`: 12 checks `[OK]`, zero `[FAIL]`.
- Endpoint autenticado: 702 processos localizados, 73 municipios, 27 sem
  localizacao e 73 itens.
- Consulta direta pelo metodo PostGIS: os mesmos totais e 73 itens.

## Evidencia visual

O arquivo `.superpowers/sdd/screenshots/visual-check.json` registra:

- 54 marcadores visiveis no filtro padrao 2026+, todos com 26x26;
- 12 tiles carregados e basemap nao vazio;
- selecao de Rondonopolis com 10 processos abertos;
- nenhum overflow horizontal em 1440, 1024 ou 390 px;
- nenhuma `pageerror`;
- 54 `path.leaflet-interactive`, correspondentes aos `CircleMarker` dos 54
  municipios, e nao a poligonos ou limites territoriais.

Os testes automatizados complementam a evidencia headless para abertura de
detalhes, acao de acompanhar e manutencao dos dados quando tiles falham.

## Segredos e credenciais

- Varredura do diff por chave privada, AWS key, token GitHub, JWT e Bearer:
  zero ocorrencias.
- Varredura dos logs recentes de `web`, `worker` e `beat` por header de
  autorizacao e atribuicoes de senha, token ou segredo: zero ocorrencias.
- Os valores `test-token` e `not-a-secret` sao fixtures sinteticas.
- A documentacao adiciona somente placeholders de configuracao.
- Login, smoke e consultas de revisao nao imprimiram senha nem token.

## Riscos residuais

- A validacao visual fornecida foi Edge headless; a homologacao humana em
  navegador e com o provedor comercial definitivo continua corretamente
  pendente.
- O fallback OSM permanece apropriado apenas para desenvolvimento. Producao
  ainda depende de provedor aprovado, atribuicao e validacao de termos/SLA.
- Permanecem 33 warnings nao bloqueantes de dependencias Python depreciadas e
  da futura dependencia `pyarrow` do pandas.
- O favicon ausente gera 404 de console ate que um asset/rota seja adicionado.
