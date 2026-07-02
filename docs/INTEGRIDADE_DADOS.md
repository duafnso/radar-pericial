# Integridade de Dados

## Objetivo

Reduzir duplicidades em producao e proteger a base contra insercoes concorrentes vindas de coletas automaticas, disparos manuais e workers Celery paralelos.

## Regras implementadas

### Movimentacoes judiciais

Chave de negocio:

- `processo_id`
- `data_movimentacao`, tratando `NULL` como data sentinela
- hash da `descricao`

Antes de criar o indice unico, o schema remove duplicatas existentes mantendo o menor `id`.

Indice:

`ux_mov_unique_business`

Gravacao:

`save_movimentacao` continua fazendo checagem previa em Python, mas tambem usa `ON CONFLICT DO NOTHING` no insert. Assim, mesmo que dois workers tentem gravar a mesma movimentacao ao mesmo tempo, o banco impede duplicidade.

### Portarias do diario oficial

Chave de negocio:

- hash do `titulo`
- `data_publicacao`
- `fonte`

Antes de criar o indice unico, o schema remove duplicatas existentes mantendo o menor `id`.

Indice:

`ux_portarias_unique_business`

Gravacao:

`save_portarias` deixou de depender somente de `DataFrame.to_sql(... append ...)` e passou a inserir por SQL parametrizado com `ON CONFLICT DO NOTHING`.

### Camadas geoespaciais recorrentes

Camadas cobertas:

- `assentamentos_incra`
- `inpe_prodes`
- `inpe_deter`
- `cadastro_ambiental`
- `desapropriacao_ativa`

Estrategia:

1. A coleta e salva em uma tabela de staging.
2. O destino remove apenas registros que batem na chave de negocio e no hash da geometria.
3. A tabela final recebe os registros novos da staging.
4. A staging e descartada.

Isso evita apagar a tabela inteira e tambem evita crescimento duplicado quando a mesma fonte retorna o mesmo poligono em coletas sucessivas.

Se o upsert por staging falhar, os dados existentes sao preservados e o append
e ignorado. A decisao evita duplicidade silenciosa; a fonte deve ser reprocessada
depois que o erro operacional for corrigido.

Chaves usadas:

- `assentamentos_incra`: `nome_pa`, `municipio`, geometria.
- `inpe_prodes`: `ano`, `classe`, geometria.
- `inpe_deter`: `view_date`, `classname`, `state`, geometria.
- `cadastro_ambiental`: `cod_imovel`, geometria.
- `desapropriacao_ativa`: `codigo_imovel`, geometria.

Observacao: quando alguma coluna de chave nao existir no payload da fonte, a rotina usa as chaves disponiveis e a geometria.

## Por que isso importa

Em ambiente comercial, o sistema pode ter:

- Celery Beat disparando coletas periodicas;
- admin disparando coletas manuais;
- retries automaticos;
- mais de um worker ativo.

Sem restricao no banco, duas execucoes proximas podem gravar o mesmo evento. A deduplicacao em Python ajuda, mas nao e suficiente contra concorrencia. A regra definitiva precisa estar no PostgreSQL.

## Cuidados

- Os indices usam `md5` para textos longos, evitando estourar limite de tamanho de indice B-tree.
- Os inserts usam SQLAlchemy `text()` com parametros nomeados.
- O upsert geoespacial usa tabelas de staging e identificadores internos controlados pelo codigo.
- Nenhum dado pessoal novo e introduzido por esta etapa.

## Proximas melhorias

- Aplicar chaves de negocio semelhantes em `eventos_administrativos`.
- Criar metricas de duplicatas descartadas por fonte.
- Criar testes com banco PostGIS real para validar upserts geoespaciais.
