# Configuracao Do GitHub

Use este checklist quando o repositorio estiver publicado no GitHub.

## Branch protection

Configure a branch principal (`main` ou `master`) com:

- Require a pull request before merging.
- Require status checks to pass before merging.
- Marque como obrigatorios:
  - `Python checks and tests`;
  - `PostgreSQL/PostGIS integration`;
  - `Frontend build`;
  - `Docker image build`.
- Require branches to be up to date before merging.
- Restrict who can push directly para a branch principal, se houver equipe.

## Secrets para smoke de homologacao

Crie em `Settings > Secrets and variables > Actions`:

- `RADAR_SMOKE_USER`: usuario usado no smoke test de homologacao.
- `RADAR_SMOKE_PASSWORD`: senha do usuario acima.

Esse usuario deve ter permissao suficiente para acessar:

- `/api/me`;
- `/api/stats`;
- `/api/processos`;
- `/api/coletas/status`;
- `/api/coletas/resumo`.

## Como rodar smoke manual

No GitHub Actions:

1. Abra o workflow `CI`.
2. Clique em `Run workflow`.
3. Preencha `smoke_base_url` com a URL de homologacao.
4. Aguarde o job `Optional homologation smoke test`.

## Regra de merge/deploy

- Nao faça merge se o CI falhar.
- Nao promova para producao sem smoke verde em homologacao.
- Nao use credenciais pessoais no smoke test; use usuario tecnico restrito.
