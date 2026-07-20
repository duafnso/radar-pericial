# Task 1 Report - Endpoint PostGIS Agregado

## Resumo
Implementado e revisado `Database.resumo_mapa_processos` e `GET /api/processos/mapa/resumo`. A consulta agrega processos por municipio antes do limite, usa coordenadas de `municipios_mt.geometry`, retorna totais localizados e sem localizacao e aceita filtros por regiao, municipio, faixa e periodo. O endpoint exige `AuthUser` e preserva respostas 401, 422, 503 e 500.

## Testes RED observados
### Implementacao original
1. O teste de banco falhou com `AttributeError` porque `resumo_mapa_processos` nao existia.
2. O teste de API falhou com `404 Not Found` antes da criacao da rota.

### Correcoes da revisao independente
1. O teste SQL falhou porque as consultas nao continham `LEFT JOIN LATERAL`, permitindo multiplicacao por scores historicos duplicados.
2. A primeira execucao dos novos testes HTTP revelou que o handler global padroniza a mensagem 503 como `Banco de dados nao inicializado`; a assercao foi alinhada ao contrato real sem alterar producao.

## Implementacao final
- Cada consulta seleciona no maximo um score por processo com `LEFT JOIN LATERAL`, ordenado por `sp.calculado_em DESC NULLS LAST, sp.id DESC`, seguido de `LIMIT 1`.
- Os itens projetam `m.nome AS municipio`, agrupando variacoes de capitalizacao no nome canonico.
- `total_municipios` usa `COUNT(DISTINCT m.nome)`.
- `faixa_dominante` desempata por score, prioridade explicita (`janela_quente`, `provavel`, `observacao`, `frio`) e ordem lexical final.
- Todos os valores de filtro continuam usando parametros SQL nomeados.
- Testes SQL verificam cardinalidade do join, nome canonico, PostGIS, todos os filtros e parametros das duas consultas.
- Testes HTTP cobrem sucesso e encaminhamento do limite, autenticacao ausente, limites 0 e 201, banco indisponivel e excecao inesperada 500.

## Comandos e resultados
- RED da revisao:
  - `python -m pytest ... -k 'resumo_mapa_processos or processos_mapa_resumo' -q`
  - Resultado: 2 falhas esperadas inicialmente; SQL sem `LEFT JOIN LATERAL` e assercao 503 desalinhada ao handler global.
- GREEN alvo no Docker com `pip install --user -r requirements-dev.txt`:
  - `7 passed, 40 deselected, 8 warnings`.
- Suite completa de escopo no Docker com dependencias dev:
  - `python -m pytest tests/test_database_methods.py tests/test_api_testclient.py -q`
  - Resultado: `47 passed, 31 warnings` em 2.03s.
- `git diff --check` e `git diff --cached --check`: exit code 0.
- `git diff --cached --name-only` antes do commit listou somente:
  - `database/db.py`
  - `tests/test_api_testclient.py`
  - `tests/test_database_methods.py`

## Commits
- Implementacao original: `60d9d9d1c2d521a6884e73dea0bd610c37bff826`.
- Correcoes da revisao: `15256f3` (`fix: make process map aggregation deterministic`).

## Arquivos alterados na revisao
- `database/db.py`
- `tests/test_api_testclient.py`
- `tests/test_database_methods.py`
- `api/main.py` foi validado pelos novos testes, mas nao exigiu alteracao.
- `.superpowers/sdd/task-1-report.md` foi atualizado depois do commit e permaneceu fora do indice.

## Riscos e observacoes
- A garantia contra scores duplicados ocorre na consulta, conforme solicitado; nenhuma migration foi adicionada.
- A consulta pressupoe a unicidade normalizada de `municipios_mt.nome`, confirmada pela revisao independente no estado atual do banco.
- Os warnings restantes sao de dependencias de terceiros (`pandas`, `passlib`, `httpx`) e nao representam falhas.
- O Python do host permanece indisponivel; toda a suite foi executada no ambiente Docker do projeto com as dependencias dev declaradas.

## Confirmacao de auto-revisao
Auto-revisao concluida. O diff final preserva o endpoint legado, nao interpola valores de entrada no SQL, aplica o limite somente depois da agregacao municipal, usa `ST_PointOnSurface`, elimina multiplicacao por score historico e nao inclui arquivos `.superpowers/sdd` no novo commit.