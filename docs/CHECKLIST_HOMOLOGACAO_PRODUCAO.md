# Checklist de Homologacao e Producao

Este checklist deve ser usado apenas depois que o produto estiver funcional localmente.

## Antes de Subir Homologacao

- Fases locais essenciais aprovadas.
- `docker compose up -d --build` funciona localmente.
- `npm run frontend:build` passa.
- Testes backend passam.
- Teste PostGIS passa no CI ou ambiente equivalente.
- Coleta DataJud real validada com amostras.
- Nenhum segredo em Git.
- `.env` local fora do versionamento.
- `DEFAULT_ADMIN_PASSWORD` usado apenas para bootstrap.
- `SECRET_KEY` forte definido.
- `SESSION_TOKEN_PEPPER` forte definido.
- `CORS_ALLOW_ORIGINS` sem `*`.

## Infraestrutura de Homologacao

- Dominio ou subdominio de homologacao.
- HTTPS.
- PostgreSQL gerenciado com PostGIS.
- Redis gerenciado.
- Logs acessiveis.
- Backups habilitados.
- Variaveis secretas configuradas fora do Git.
- `ENABLE_API_DOCS=false`, salvo necessidade controlada.

## Depois de Subir Homologacao

- Aplicar migracoes.
- Rodar smoke test autenticado.
- Executar login admin.
- Trocar senha admin inicial.
- Remover `DEFAULT_ADMIN_PASSWORD`.
- Rodar coleta judicial controlada.
- Validar `/api/coletas/resumo`.
- Validar Radar de Processos.
- Validar Mapa.
- Validar Usuarios.
- Validar Auditoria.
- Validar Alertas.

## Antes de Producao

- Revisao juridica das fontes concluida.
- Termos de uso prontos.
- Politica de privacidade pronta.
- Backup e restore testados.
- Plano de resposta a incidente definido.
- Branch protection ativo.
- CI obrigatorio para merge.
- Secrets `RADAR_SMOKE_USER` e `RADAR_SMOKE_PASSWORD` configurados.
- Smoke test de homologacao aprovado.

## Criterio de Bloqueio

Nao publicar se qualquer item abaixo falhar:

- health/readiness;
- login;
- assets frontend;
- banco;
- Redis;
- coleta DataJud;
- permissao admin;
- smoke test;
- backup;
- restore;
- revisao juridica minima.
