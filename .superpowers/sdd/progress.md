# Progresso SDD - Mapa Territorial

Plano: `docs/superpowers/plans/2026-07-19-mapa-territorial.md`
Base inicial: `273e69f7cb09aa3877f75deee45720aa54198ea2`
Branch: `codex/finish-production-plan`

## Tarefas

- [x] 1. Endpoint PostGIS agregado e testes
- [x] 2. Leaflet local e modelo de dados do mapa
- [x] 3. Modal de processo reutilizavel
- [x] 4. Novo mapa, painel lateral e responsividade
- [x] 5. Docker, dados reais, regressao e documentacao

## Observacoes

- O workspace possui alteracoes anteriores nao commitadas que fazem parte do plano tecnico.
- Cada tarefa deve preservar alteracoes fora do seu escopo.
- Implementacao, testes e revisao sao obrigatorios antes de avancar.
- Tarefa 1 aprovada apos correcoes de cardinalidade, nome canonico e desempate.
- Evidencia da tarefa 1: 47 testes de escopo aprovados no Docker.

- Tarefa 2 aprovada: 4 testes frontend e build Vite aprovados.

- Tarefa 3 aprovada: modal compartilhado com foco, teclado e 8 testes frontend.

- Tarefa 4 aprovada: 51 testes backend, 14 contratos, 26 testes comportamentais, typecheck e build.

- Tarefa 5 aprovada: 85 backend, 41 frontend, smoke 12/12, totais PostGIS/API conferidos.
