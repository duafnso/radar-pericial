# Pendencias de Producao

Estado revisado em 2026-07-13.

## Concluido nesta fase

- Build Docker reprodutivel com frontend React/Vite gerado automaticamente.
- `docker compose up -d --build` validado com web, worker, beat, PostgreSQL/PostGIS e Redis.
- Health checks validados:
  - `/health`
  - `/health/ready`
- Smoke test autenticado validado:
  - login admin;
  - `/api/me`;
  - `/api/stats`;
  - `/api/processos`;
  - `/api/coletas/status`;
  - `/api/coletas/resumo`;
  - HTML e assets do frontend.
- Suite automatizada validada no container: 53 testes passando.
- Producao agora falha no startup se faltarem segredos fortes:
  - `SECRET_KEY`;
  - `SESSION_TOKEN_PEPPER`;
  - `DATAJUD_API_KEY`.
- CORS continua bloqueando `*` em producao.
- Coletas manuais agora bloqueiam execucao duplicada do mesmo tipo quando ja existe coleta em andamento.
- Coleta DataJud ganhou suporte a janela incremental quando `DATAJUD_START_DATE` nao estiver fixado.
- Coleta DataJud registra metricas por classe em `metricas_coleta_classe`:
  - registros coletados;
  - registros salvos;
  - descartados sem CNJ;
  - duplicados;
  - erro resumido.
- Radar de Processos ganhou filtros de data de distribuicao.
- Radar de Processos ganhou paginacao e exportacao CSV da pagina atual.
- Usuarios ganhou filtros por perfil, status e busca.
- Usuarios ganhou exportacao CSV da lista filtrada.
- Auditoria ganhou filtros por acao, ator, entidade e intervalo de datas.
- Auditoria ganhou exportacao CSV da lista filtrada.
- Central de Alertas permite marcar alerta de processo acompanhado como lido.

## Ainda pendente antes de venda comercial

### 1. Homologacao de producao

- Escolher provedor de hospedagem.
- Configurar dominio real.
- Configurar HTTPS.
- Configurar PostgreSQL gerenciado com PostGIS.
- Configurar Redis gerenciado.
- Definir backup diario do banco.
- Definir retencao de logs.
- Subir ambiente de homologacao separado do ambiente local.
- Rodar smoke test no dominio final.

### 2. Seguranca operacional

- Remover `DEFAULT_ADMIN_PASSWORD` depois do bootstrap inicial.
- Definir rotacao periodica para `SECRET_KEY` e `SESSION_TOKEN_PEPPER`.
- Revisar tempo de expiracao de sessoes para o modelo comercial.
- Adicionar rate limit persistente, preferencialmente via Redis, para login e acoes sensiveis.
- Criar politica de retencao para:
  - `auditoria_eventos`;
  - `execucoes_coleta`;
  - `metricas_coleta_classe`;
  - sessoes expiradas.

### 3. Banco e migracoes

- Adotar Alembic ou migracoes SQL versionadas.
- Separar schema inicial de alteracoes incrementais.
- Criar rotina de backup antes de aplicar migracoes.
- Testar restore em ambiente local/homologacao.
- Criar testes de integracao com PostGIS em CI.

### 4. DataJud e qualidade de dados

- Depois do backfill de 2026, remover ou deixar vazio `DATAJUD_START_DATE` para ativar coleta incremental real por ultima data conhecida.
- Validar juridicamente o uso comercial da API DataJud/CNJ.
- Definir limites finais de coleta para producao:
  - `DATAJUD_CLASSES_LIMIT`;
  - `DATAJUD_PAGE_SIZE`;
  - `DATAJUD_MAX_RESULTS_PER_CLASS`;
  - `DATAJUD_REQUEST_DELAY_SECONDS`;
  - `DATAJUD_INCREMENTAL_OVERLAP_DAYS`.
- Monitorar 429 e reduzir agressividade se a API limitar chamadas.
- Criar rotina de auditoria de qualidade dos processos:
  - CNJ ausente;
  - municipio ausente;
  - comarca nao mapeada;
  - score sem explicacao suficiente.

### 5. Produto e frontend

- Validar visualmente as telas em desktop, notebook e mobile.
- Melhorar estados vazios com acoes diretas por perfil.
- Criar exportacao CSV para Coletas.
- Avaliar se a exportacao CSV do Radar de Processos deve baixar todos os resultados filtrados ou apenas a pagina atual.

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

### 7. CI/CD

- Criar pipeline automatizado com:
  - `python -m py_compile`;
  - `pytest`;
  - `npm run frontend:build`;
  - build Docker;
  - smoke test em homologacao.
- Bloquear deploy se qualquer etapa falhar.
