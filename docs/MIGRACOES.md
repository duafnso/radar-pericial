# Migracoes De Banco

O projeto agora possui uma base de migracoes SQL versionadas em `database/migrations`.

## Estado atual

O schema historico ainda e inicializado por `database/db.py`.

As migracoes SQL foram introduzidas como caminho de transicao para tirar alteracoes incrementais do startup da aplicacao, sem quebrar o ambiente atual.

O nucleo operacional ja esta versionado ate `0007`:

- coletas e metricas;
- usuarios, sessoes e auditoria;
- processos acompanhados e alertas;
- movimentacoes, publicacoes, eventos administrativos, score, portarias e data lake;
- explicacao auditavel do score;
- normalizacao controlada de municipios observados no DataJud.

O schema geoespacial e as tabelas de referencia ainda permanecem parcialmente no bootstrap legado.

## Como aplicar

Com as variaveis `DATABASE_URL` ou `PG*` apontando para o banco correto:

```bash
python tools/apply_migrations.py
```

Para validar quais arquivos seriam considerados, sem aplicar:

```bash
MIGRATIONS_DRY_RUN=true python tools/apply_migrations.py
```

## Controle de versao

O script cria a tabela:

```sql
schema_migrations (
    version TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

Cada arquivo `.sql` aplicado fica registrado por nome e checksum.

Se uma migracao ja aplicada for alterada, o script falha por divergencia de checksum. A regra correta e criar um novo arquivo de migracao.

## Convencao de nomes

Use nomes ordenaveis:

```text
0001_baseline_schema_marker.sql
0002_operational_collection_metrics.sql
0003_nome_da_mudanca.sql
```

## Proximo passo tecnico

Quando o schema estabilizar, ha duas opcoes:

- manter SQL versionado simples, suficiente para este projeto enquanto as mudancas forem controladas;
- migrar para Alembic se houver muitos ambientes, branching frequente ou necessidade de autogeracao baseada em modelos SQLAlchemy.

Antes de aplicar qualquer migracao em producao:

- fazer backup do PostgreSQL;
- aplicar primeiro em homologacao;
- rodar smoke test;
- validar logs do `web`;
- validar telas principais.

## Decisao atual: SQL versionado antes de Alembic

Neste momento, a melhor escolha e continuar com SQL versionado simples. O projeto ainda tem poucas migracoes, o schema legado continua parcialmente em `database/db.py`, e o ganho de Alembic nao compensa o custo de migracao agora.

Reavaliar Alembic quando ocorrer pelo menos uma destas condicoes:

- muitos ambientes independentes com historico divergente;
- necessidade frequente de downgrade controlado;
- modelos SQLAlchemy virarem a fonte principal do schema;
- conflitos recorrentes de migracao entre branches.