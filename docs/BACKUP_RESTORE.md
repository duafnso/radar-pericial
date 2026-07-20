# Backup e Restore

Este documento define a rotina operacional minima de backup e restore para o Radar Pericial.

## Objetivo

Evitar perda de dados e garantir que qualquer migracao ou deploy possa ser revertido com segurança operacional.

## Requisitos

Na maquina que executa o backup/restore, os binarios abaixo precisam estar disponiveis:

- `pg_dump`;
- `pg_restore`;
- `psql`.

O script usa `DATABASE_URL` ou as variaveis:

- `PGHOST`;
- `PGPORT`;
- `PGUSER`;
- `PGPASSWORD`;
- `PGDATABASE`.

Senha nao e impressa e nao e passada como argumento de linha de comando; ela e enviada via variavel `PGPASSWORD` para o processo filho.

## Backup Local

Com o banco local rodando:

```bash
python tools/backup_db.py --output-dir backups
```

Para gerar SQL texto:

```bash
python tools/backup_db.py --output-dir backups --plain-sql
```

Formato recomendado para operacao: `.dump`, por ser mais apropriado para restore com `pg_restore`.

## Restore Local

Antes de restaurar, confirme que o alvo e um banco local ou de homologacao. Nunca rode restore em producao sem janela controlada, backup novo e aprovacao.

```bash
python tools/restore_db.py backups/arquivo.dump
```

Para automacao controlada:

```bash
python tools/restore_db.py backups/arquivo.dump --yes
```

## Rotina Antes de Migracoes

Antes de aplicar migracoes em homologacao ou producao:

1. Rodar backup.
2. Guardar arquivo fora do container.
3. Registrar data, ambiente, responsavel e versao do commit.
4. Aplicar migracoes.
5. Rodar smoke test.
6. Validar logs do `web`, `worker` e `beat`.

## Politica Recomendada

Local:

- backup manual antes de migracoes relevantes;
- manter ultimos 5 backups locais.

Homologacao:

- backup diario automatico se o provedor permitir;
- backup manual antes de deploy.

Producao:

- backup diario automatico;
- retencao minima de 30 dias;
- teste de restore mensal;
- backup manual antes de migracoes;
- restauracao somente com aprovacao e janela definida.

## Checklist de Restore Testado

Um restore so deve ser considerado testado quando:

- o banco restaurado sobe;
- `python tools/apply_migrations.py` nao falha;
- `/health/ready` passa;
- login funciona;
- `/api/processos?limit=5` responde;
- `/api/coletas/resumo` responde;
- a interface abre no navegador.
