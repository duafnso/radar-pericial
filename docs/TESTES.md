# Testes - Radar Pericial

Este projeto separa dependencias de runtime e dependencias de desenvolvimento.

## Dependencias

Runtime de producao:

```bash
pip install -r requirements.txt
```

Ambiente de desenvolvimento/testes:

```bash
pip install -r requirements-dev.txt
```

## Rodando a suite

```bash
pytest
```

Checagem de sintaxe usada no CI:

```bash
python -m py_compile api/main.py database/db.py alerts/scheduler.py collector/judicial_collector.py collector/admin_collector.py collector/multi_source_collector.py etl/geospatial_etl.py intelligence/taxonomy.py tools/apply_migrations.py tools/smoke_test.py
```

Build frontend:

```bash
npm ci
npm run frontend:build
```

Para rodar um arquivo especifico:

```bash
pytest tests/test_runtime_config.py
```

## Cobertura inicial

A primeira suite cobre regras criticas de configuracao:

- `SESSION_TOKEN_PEPPER` ou `SECRET_KEY` obrigatorio em producao.
- `CORS_ALLOW_ORIGINS` obrigatorio em producao.
- wildcard `*` bloqueado em CORS de producao.
- documentacao OpenAPI desabilitada por padrao em producao.
- throttle basico de tentativas de login.
- matriz de permissoes por role.
- dependencias de permissao para aceitar/rejeitar usuarios.

## Observacao

Os testes de integracao com banco devem rodar em ambiente com PostgreSQL/PostGIS.
Nao use o banco de producao para testes automatizados.

Para executar os testes de integracao localmente contra um PostgreSQL/PostGIS de teste:

```bash
RUN_POSTGIS_INTEGRATION=true DATABASE_URL=postgresql://usuario:senha@localhost:5432/radar_pericial_test pytest tests/integration
```

## CI

O workflow `.github/workflows/ci.yml` executa:

- `py_compile`;
- `pytest`;
- `npm run frontend:build`;
- teste de integracao PostgreSQL/PostGIS;
- docker build;
- smoke test opcional em homologacao por execucao manual.
