# Pendencias de Producao

Estado revisado em 2026-07-19.

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
  - `/api/qualidade/processos`;
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
- Auditoria tecnica de qualidade dos processos implementada em base local:
  - endpoint `/api/qualidade/processos`;
  - verificacao de CNJ, municipio, comarca, mapeamento municipal, score e datas;
  - teste de API e teste unitario de banco;
  - validacao real com 729 processos;
  - indice de qualidade elevado de 81 para 92 apos persistir explicacoes dos scores e normalizar municipios.
- Dashboard React passou a exibir qualidade dos dados, problemas ativos e recomendacao operacional.
- Explicacao auditavel do score adicionada ao motor, banco e recalculo dos 729 processos.
- Migracoes `0005_judicial_intelligence`, `0006_score_explanation` e `0007_normalize_process_municipalities` aplicadas e validadas no PostGIS local.
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
- Scripts de backup/restore criados em `tools/backup_db.py` e `tools/restore_db.py`.
- Checklist de homologacao/producao criado em `docs/CHECKLIST_HOMOLOGACAO_PRODUCAO.md`.
- Matriz inicial de compliance de fontes criada em `docs/COMPLIANCE_FONTES_DADOS.md`.

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
- Rotina operacional de backup/restore documentada e scripts locais criados; ainda falta testar restore em homologacao real.

### 3. Banco e migracoes

- Continuar migrando apenas o schema geoespacial e tabelas de referencia restantes de `database/db.py`; o nucleo judicial, score, usuarios, auditoria e coletas ja esta versionado.
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
- Tratar as inconsistencias restantes observadas na auditoria real:
  - manter 27 processos de segunda instancia sem municipio ate existir fonte confiavel para localizacao;
  - definir se os 559 registros anteriores a 2026 devem permanecer no acervo historico ou sair do indicador operacional.

### 5. Produto e frontend

- Validar visualmente as telas em desktop, notebook e mobile.
- Refinar microcopy e estados guiados apos teste com usuario real.
- Revisar se exportacoes grandes precisam de processamento assincrono no futuro.

### 6. Compliance comercial

- Matriz inicial de fontes criada em `docs/COMPLIANCE_FONTES_DADOS.md`; ainda falta revisao juridica final com:
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
## Mapa territorial

Validado localmente em 2026-07-20 com dados reais do PostGIS.

### Concluido
- Leaflet e CSS cartografico entram no bundle Vite local, sem CDN em tempo de execucao.
- `GET /api/processos/mapa/resumo` agrega processos por municipio antes de aplicar `limit_cidades`.
- Smoke autenticado valida o contrato agregado e suas quatro chaves obrigatorias.
- Totais validados: 702 processos localizados, 73 municipios e 27 processos sem localizacao.
- Docker, suite backend com PostGIS e bundle frontend foram validados localmente.

### Pendente antes do uso comercial
- Escolher um provedor comercial de tiles com SLA e termos compativeis.
- Usar URL e atribuicao do provedor em `VITE_MAP_TILE_URL` e `VITE_MAP_TILE_ATTRIBUTION`.
- Trocar o provedor exige rebuild da imagem Docker, pois o Vite incorpora essas variaveis no bundle.
- Manter a atribuicao visivel no mapa; ela e obrigatoria para qualquer provedor.
- Validar os termos e a disponibilidade do provedor escolhido em homologacao e producao.

### Checklist visual pendente
- Edge headless foi executado nesta validacao; ainda falta homologacao humana em navegador real e com o provedor comercial definitivo.
- Conferir basemap, marcadores compactos e contadores em 1440x900, 1024x768 e 390x844.
- Confirmar selecao municipal, detalhes e acompanhamento de processo.
- Simular falha de tiles e confirmar que os dados municipais continuam visiveis.
- Confirmar ausencia de poligonos ou limites territoriais.
- Confirmar ausencia de sobreposicao ou corte de textos e controles.
