# CI/CD

O projeto possui workflow em `.github/workflows/ci.yml`.

## O que o CI valida

- Instala dependencias Python de desenvolvimento.
- Compila os principais modulos Python com `py_compile`.
- Executa pytest.
- Sobe PostgreSQL/PostGIS no GitHub Actions, aplica migracoes e roda 	ests/integration.
- Instala dependencias Node com `npm ci`.
- Executa `npm run frontend:build`.
- Constroi a imagem Docker com `docker build`.

## Smoke test de homologacao

O workflow tambem aceita execucao manual (`workflow_dispatch`) com a URL de homologacao.

Secrets esperados no GitHub:

- `RADAR_SMOKE_USER`
- `RADAR_SMOKE_PASSWORD`

Entrada manual:

- `smoke_base_url`: URL publica da homologacao, por exemplo `https://homologacao.seu-dominio.com.br`.

O smoke test valida:

- `/health`;
- `/health/ready`;
- frontend HTML e asset estatico;
- login;
- `/api/me`;
- `/api/stats`;
- `/api/processos`;
- `/api/coletas/status`;
- `/api/coletas/resumo`.

## Regra de deploy

Nao promova uma versao para producao se qualquer job do CI falhar.

Em producao, mantenha:

- `APP_ENV=production`;
- `ENABLE_API_DOCS=false`, salvo necessidade controlada;
- `CORS_ALLOW_ORIGINS` com dominio real;
- `SECRET_KEY` forte;
- `SESSION_TOKEN_PEPPER` forte;
- `DATAJUD_API_KEY` configurada.

## Branch protection

No GitHub, configure a branch principal para exigir estes checks antes de merge:

- `Python checks and tests`;
- `PostgreSQL/PostGIS integration`;
- `Frontend build`;
- `Docker image build`.

Ative tambem bloqueio de merge com conversas nao resolvidas e pull request obrigatorio para mudancas na branch principal.