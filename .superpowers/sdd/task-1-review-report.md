# Task 1 Re-review Report - Mapa Territorial

## Veredito

**APPROVED**

## Achados remanescentes

### Critical

Nenhum.

### Important

Nenhum.

### Minor

Nenhum bloqueante.

## Verificacao dos achados anteriores

1. **Cardinalidade de `score_pericial`: corrigida.**
   - `database/db.py:1636-1643` e `database/db.py:1678-1685` usam `LEFT JOIN LATERAL` com `ORDER BY sp.calculado_em DESC NULLS LAST, sp.id DESC LIMIT 1`.
   - Cada processo contribui com no maximo um score nas consultas de itens e totais, mesmo que existam linhas historicas duplicadas.

2. **Agregacao pelo municipio canonico: corrigida.**
   - `database/db.py:1633` projeta `m.nome AS municipio`.
   - Os itens agrupam pelo nome canonico e geometria, e `database/db.py:1674` calcula `total_municipios` com `COUNT(DISTINCT m.nome)`.
   - Variacoes de capitalizacao em `p.municipio` deixam de gerar itens municipais separados.

3. **Desempate de `faixa_dominante`: corrigido.**
   - `database/db.py:1654-1664` ordena por score, prioridade explicita das faixas e ordem lexical final.
   - O resultado e deterministico quando processos possuem o mesmo `score_total`.

4. **Cobertura HTTP: corrigida.**
   - `tests/test_api_testclient.py:606` protege ausencia de autenticacao com HTTP 401.
   - `tests/test_api_testclient.py:614` protege limites `0` e `201` com HTTP 422.
   - `tests/test_api_testclient.py:629` protege banco indisponivel com HTTP 503.
   - `tests/test_api_testclient.py:647` protege excecoes inesperadas com HTTP 500.
   - O teste de sucesso tambem verifica todos os filtros e o encaminhamento de `limit_cidades`.

5. **Cobertura SQL/PostGIS: corrigida.**
   - `tests/test_database_methods.py:364-442` verifica o join lateral deterministico nas duas consultas, nome canonico, desempate, `ST_PointOnSurface`, filtros, parametros nomeados e exclusao do limite da consulta de totais.

## Testes e evidencias

- Evidencia Docker registrada em `.superpowers/sdd/task-1-report.md`: **47 passed, 31 warnings** na suite completa de `tests/test_database_methods.py` e `tests/test_api_testclient.py`.
- Evidencia Docker dos testes-alvo: **7 passed, 40 deselected, 8 warnings**.
- `git diff --check` e `git diff --cached --check`: exit code 0, conforme o relatorio atualizado.
- Nesta re-revisao, o diff completo e os trechos atuais de producao e testes foram inspecionados. A suite nao foi reexecutada independentemente, conforme orientacao para nao aguardar dependencias ausentes.

## Riscos residuais

- A aprovacao usa como evidencia de execucao os resultados Docker documentados pelo implementador; nao houve segunda execucao independente nesta re-revisao.
- O schema ainda permite multiplas linhas em `score_pericial` por processo. O endpoint revisado permanece correto porque seleciona deterministicamente apenas a linha mais recente, mas outros consumidores da tabela fora do escopo podem ter semantica diferente.
- A consulta pressupoe unicidade normalizada de `municipios_mt.nome`; a revisao anterior confirmou zero nomes normalizados duplicados no banco entao existente.