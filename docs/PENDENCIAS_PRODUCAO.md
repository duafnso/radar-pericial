# Pendencias de Producao

Estado revisado em 2026-07-13.

## Concluido nesta fase

- Build Docker reprodutivel com frontend React/Vite gerado automaticamente.
- `docker compose up -d --build` validado com web, worker, beat, PostgreSQL/PostGIS e Redis.
- Health checks validados:
  - `/health`;
  - `/health/ready`.
- Smoke test autenticado validado:
  - login admin;
  - `/api/me`;
  - `/api/stats`;
  - `/api/processos`;
  - `/api/coletas/status`;
  - `/api/coletas/resumo`;
  - HTML e assets do frontend.
- Suite automatizada validada no container.
- Producao falha no startup se faltarem segredos fortes:
  - `SECRET_KEY`;
  - `SESSION_TOKEN_PEPPER`;
  - `DATAJUD_API_KEY`.
- CORS bloqueia `*` em producao.
- Bootstrap admin ficou mais seguro:
  - `DEFAULT_ADMIN_PASSWORD` cria o admin apenas se ele nao existir;
  - reset automatico de senha exige `RESET_DEFAULT_ADMIN_PASSWORD=true`;
  - reset via startup e ignorado em producao.
- Rate limit de login e acoes sensiveis implementado com backend configuravel:
  - memoria em desenvolvimento/testes;
  - Redis por padrao em producao.
- Coletas manuais agora possuem limite de abuso e bloqueio de execucao duplicada do mesmo tipo quando ja existe coleta em andamento.
- Coleta DataJud ganhou suporte a janela incremental quando `DATAJUD_START_DATE` nao estiver fixado.
- Coleta DataJud registra metricas por classe em `metricas_coleta_classe`:
  - registros coletados;
  - registros salvos;
  - descartados sem CNJ;
  - duplicados;
  - erro resumido.
- Politica tecnica de retencao operacional implementada por task agendada:
  - `auditoria_eventos`;
  - `execucoes_coleta`;
  - `metricas_coleta_classe`.
- Radar de Processos ganhou filtros de data de distribuicao, paginacao e CSV:
  - pagina atual;
  - todos os resultados filtrados.
- Usuarios ganhou filtros por perfil, status e busca.
- Usuarios ganhou exportacao CSV da lista filtrada.
- Auditoria ganhou filtros por acao, ator, entidade e intervalo de datas.
- Auditoria ganhou exportacao CSV da lista filtrada.
- Coletas ganhou exportacao CSV de historico e metricas.
- Coletas ganhou diagnostico por classe/fonte com coletados, salvos, sem CNJ e duplicados.
- Central de Alertas permite marcar alerta de processo acompanhado como lido.
- CI criado em `.github/workflows/ci.yml` com:
  - `py_compile`;
  - `pytest`;
  - teste de integracao PostgreSQL/PostGIS;
  - build frontend;
  - build Docker;
  - smoke test opcional de homologacao.
- Base de migracoes SQL versionadas criada em `database/migrations`.
- Script `tools/apply_migrations.py` criado com controle em `schema_migrations`.
- Documentacao de GitHub, CI/CD, migracoes e testes criada/atualizada.

## Plano de evolucao local antes do lancamento

Antes de iniciar hospedagem publica e venda comercial, seguir o roadmap em docs/PLANO_EVOLUCAO_PRODUTO.md. A prioridade e validar localmente as fases essenciais: consolidacao do MVP, inteligencia de presenca, perfil profissional, busca interna de peritos e CRM pericial simples.

## Ainda pendente antes de venda comercial

### 1. Homologacao de producao

- Escolher provedor de hospedagem.
- Configurar dominio real.
- Configurar HTTPS.
- Configurar PostgreSQL gerenciado com PostGIS.
- Configurar Redis gerenciado.
- Definir backup diario do banco.
- Definir retencao de logs da infraestrutura.
- Subir ambiente de homologacao separado do ambiente local.
- Rodar smoke test no dominio final.

### 2. Seguranca operacional

- Remover `DEFAULT_ADMIN_PASSWORD` do ambiente depois do bootstrap inicial.
- Executar e documentar o primeiro ciclo real de rotacao de `SECRET_KEY` e `SESSION_TOKEN_PEPPER` em homologacao.
- Revisar tempo de expiracao de sessoes para o modelo comercial.
- Validar rate limit com Redis no ambiente real de homologacao.
- Definir rotina operacional para backup/restore antes de migracoes.

### 3. Banco e migracoes

- Continuar migrando alteracoes incrementais de `database/db.py` para arquivos em `database/migrations`.
- Testar restore em ambiente local/homologacao.
- Avaliar Alembic somente se o volume de migracoes, ambientes ou branches tornar o SQL versionado simples custoso.

### 4. DataJud e qualidade de dados

- Depois do backfill de 2026, remover ou deixar vazio `DATAJUD_START_DATE` para ativar coleta incremental real por ultima data conhecida.
- Validar juridicamente o uso comercial da API DataJud/CNJ.
- Definir limites finais de coleta para producao:
  - `DATAJUD_CLASSES_LIMIT`;
  - `DATAJUD_PAGE_SIZE`;
  - `DATAJUD_MAX_RESULTS_PER_CLASS`;
  - `DATAJUD_REQUEST_DELAY_SECONDS`;
  - `DATAJUD_INCREMENTAL_OVERLAP_DAYS`.
- Monitorar `429 Too Many Requests` e reduzir agressividade se a API limitar chamadas.
- Criar rotina de auditoria de qualidade dos processos:
  - CNJ ausente;
  - municipio ausente;
  - comarca nao mapeada;
  - score sem explicacao suficiente.

### 5. Produto e frontend

- Validar visualmente as telas em desktop, notebook e mobile.
- Refinar microcopy e estados guiados apos teste com usuario real.
- Revisar se exportacoes grandes precisam de processamento assincrono no futuro.

### 6. Compliance comercial

- Criar matriz de fontes de dados com:
  - licenca;
  - permissao de armazenamento;
  - permissao de redistribuicao;
  - risco LGPD;
  - politica de retencao.
- Revisar DataJud/CNJ antes de uso comercial.
- Revisar CAR/SICAR, SIGEF, INPE, DOU, DNIT e SINFRA.
- Criar termos de uso.
- Criar politica de privacidade.
- Definir contrato e aviso de responsabilidade sobre decisao pericial.

### 7. GitHub e CI/CD

- Subir o repositorio para GitHub, se ainda nao estiver remoto.
- Configurar branch protection para exigir CI verde antes de merge.
- Configurar secrets `RADAR_SMOKE_USER` e `RADAR_SMOKE_PASSWORD`.
- Rodar workflow de CI em pull request.
- Rodar smoke test manual contra homologacao antes do primeiro deploy comercial.
- Bloquear deploy se qualquer etapa falhar.