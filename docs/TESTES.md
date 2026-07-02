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
