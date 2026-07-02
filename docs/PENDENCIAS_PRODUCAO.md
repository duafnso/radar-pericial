# Pendencias de Producao

Este checklist ordena as pendencias por importancia tecnica e custo relativo.

## Prioridade 1 - Baixo custo, alto impacto

- Instalar dependencias de desenvolvimento e rodar `pytest`.
- Subir Docker local com PostGIS, Redis, web, worker e beat.
- Validar `_init_schema` em PostgreSQL/PostGIS real.
- Validar login, logout, troca de senha, usuarios, auditoria e coletas manuais.
- Validar upserts de `parcelas_sigef`, `inpe_prodes`, `inpe_deter`, `cadastro_ambiental`, `assentamentos_incra` e `desapropriacao_ativa`.
- Corrigir qualquer erro de SQL/PostGIS encontrado na primeira subida.

## Prioridade 2 - Segurança e integridade

- Confirmar `APP_ENV=production`, `SECRET_KEY`, `SESSION_TOKEN_PEPPER` e `CORS_ALLOW_ORIGINS` no ambiente real.
- Manter `ENABLE_API_DOCS=false` em producao.
- Manter `ALLOW_INSECURE_CAR_SSL=false` em producao.
- Revisar `DEFAULT_ADMIN_PASSWORD`: usar apenas no bootstrap e remover depois.
- Criar politica de retencao para `auditoria_eventos`, `execucoes_coleta` e sessoes expiradas.

## Prioridade 3 - Migração de banco

- Adotar Alembic ou migrações SQL versionadas.
- Separar schema inicial de alteracoes incrementais.
- Criar rotina de backup antes de aplicar migracoes.
- Testar restore em ambiente local/homologacao.

## Prioridade 4 - Testes

- Criar testes de API com `httpx`/FastAPI TestClient.
- Criar testes de permissao para endpoints reais.
- Criar testes de auditoria.
- Criar testes de troca de senha.
- Criar testes de coletores com `requests-mock`.
- Criar testes de integracao com banco PostGIS via Docker.

## Prioridade 5 - Compliance e comercial

- Criar matriz de fontes de dados com licenca, permissao de armazenamento, permissao de redistribuicao e risco LGPD.
- Revisar DataJud/CNJ antes de uso comercial.
- Revisar CAR/SICAR, SIGEF, INPE, DOU, DNIT e SINFRA.
- Definir politica de cache e retenção por fonte.
- Criar termos de uso e politica de privacidade.

## Prioridade 6 - Produto

- Implementar edicao real de peritos ou manter a acao oculta.
- Adicionar filtros na tela de auditoria.
- Adicionar filtros por role/status na tela de usuarios.
- Melhorar mensagens de erro por perfil.
- Criar exportacao CSV para auditoria e coletas.

## Prioridade 7 - Operacao

- Adicionar logs estruturados.
- Adicionar metricas de sucesso/falha por fonte.
- Adicionar alerta quando coleta falhar repetidamente.
- Configurar backup automatico do PostgreSQL.
- Criar CI/CD com py_compile, pytest, validacao JS e build Docker.
