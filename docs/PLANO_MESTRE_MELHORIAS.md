# Plano Mestre de Melhorias - Radar Pericial

Estado: plano consolidado para desenvolvimento local.

Premissa: o Radar Pericial deve continuar rodando localmente enquanto as funcionalidades essenciais sao construidas, testadas e aprovadas. Homologacao online, venda comercial e lancamento publico so entram depois da validacao local.

## Objetivo Final

Transformar o Radar Pericial em uma plataforma completa para o mercado pericial rural e territorial:

- inteligencia de oportunidades judiciais e administrativas;
- perfil profissional do perito;
- busca de peritos por contratantes;
- CRM pericial para acompanhamento de oportunidades;
- gestao de escritorios e equipes;
- inteligencia estrategica regional;
- marketplace pericial controlado;
- preparacao posterior para operacao comercial online.

## Principios de Execucao

- Construir primeiro o que aproveita a base atual.
- Priorizar funcionalidades validaveis localmente.
- Evitar infraestrutura cara antes do produto estar funcional.
- Separar claramente MVP local, homologacao e producao.
- Manter Docker local como ambiente padrao de teste.
- Manter FastAPI, PostgreSQL/PostGIS, Redis, Celery e React/Vite/TypeScript.
- Criar migracoes SQL versionadas para novas tabelas.
- Adicionar testes a cada modulo novo.
- Nao criar tela falsa: toda tela nova deve ter persistencia e API real.

## Fase 0 - Consolidacao do MVP Atual

Complexidade: baixa.
Prioridade: critica.
Status esperado antes de novas features: estavel.

### Melhorias

- Revisar textos, acentos e mensagens quebradas.
- Validar logo, sidebar, login e identidade visual.
- Validar dashboard, mapa, radar, coletas, usuarios, auditoria e alertas.
- Confirmar que o Docker sobe localmente com `docker compose up -d --build`.
- Confirmar que o frontend builda com `npm run frontend:build`.
- Confirmar que testes backend passam.
- Confirmar que `/health` e `/health/ready` passam.
- Conferir se DataJud salva processos reais e metricas.
- Conferir se os processos filtrados por 2026+ aparecem no Radar.

### Entregaveis

- Checklist local de validacao.
- Relatorio de coleta DataJud com:
  - registros coletados;
  - registros salvos;
  - duplicados;
  - descartados sem CNJ;
  - erros 401, 429 ou timeout.
- Interface sem textos corrompidos.
- Build Docker funcional.

### Criterios de aceite

- Login admin funciona.
- Navegacao pela sidebar funciona.
- Radar de Processos lista dados reais quando existem dados no banco.
- Mapa nao fica branco sem mensagem de fallback.
- Coletas mostram historico e diagnostico.
- Testes automatizados passam.

## Fase 1 - Inteligencia de Presenca Essencial

Complexidade: baixa a media.
Prioridade: alta.
Publico principal: peritos e assistentes tecnicos.

### Objetivo

Responder rapidamente: onde o perito deve investir tempo, deslocamento e presenca profissional?

### Melhorias

- Criar ranking semanal de oportunidades.
- Criar bloco `Top 10 oportunidades da semana`.
- Criar filtros por tema:
  - desapropriacao;
  - servidao;
  - ambiental;
  - seguro agricola;
  - usucapiao;
  - avaliacao rural.
- Melhorar mapa por municipio/comarca.
- Criar resumo de areas aquecidas.
- Criar indicador de ultima coleta usada.
- Melhorar alertas por tema, comarca e regiao.
- Adicionar painel `Onde atuar agora`.

### Backend

- Criar endpoint `GET /api/oportunidades/ranking`.
- Criar endpoint `GET /api/oportunidades/areas-aquecidas`.
- Criar endpoint `GET /api/oportunidades/temas`.
- Reutilizar score atual e dados de processos.
- Adicionar parametros:
  - periodo;
  - tema;
  - regiao;
  - municipio;
  - comarca;
  - limite.

### Frontend

- Atualizar Dashboard.
- Atualizar Radar de Processos.
- Criar componente de ranking.
- Criar chips/filtros por tema.
- Criar estados vazios orientados:
  - sem dados;
  - coleta pendente;
  - filtros restritivos;
  - DataJud sem resposta.

### Testes

- Testar ranking com dados fake.
- Testar filtros por tema.
- Testar resposta vazia.
- Testar permissao de acesso.

### Criterios de aceite

- Usuario ve as 10 melhores oportunidades sem precisar analisar a lista inteira.
- Filtros por tema funcionam.
- Dashboard mostra areas aquecidas.
- Dados informam recorte temporal e origem.

## Fase 2 - Perfil Profissional do Perito

Complexidade: media.
Prioridade: alta.
Publico principal: peritos e assistentes tecnicos.

### Objetivo

Criar o ativo central da plataforma: um perfil profissional vivo, alimentado pelo perito e reutilizavel futuramente no marketplace.

### Melhorias

- Criar tela `Meu Perfil Profissional`.
- Cadastrar dados profissionais:
  - nome completo;
  - titulo profissional;
  - registro profissional;
  - telefone profissional;
  - email profissional;
  - cidade base;
  - regioes de atuacao;
  - mini bio;
  - visibilidade do perfil.
- Cadastrar especialidades.
- Cadastrar experiencias periciais.
- Permitir marcar experiencia como publica ou privada.
- Mostrar completude do perfil.

### Banco

- Criar `perfis_profissionais`.
- Criar `perfil_especialidades`.
- Criar `experiencias_periciais`.
- Criar indices por:
  - usuario;
  - especialidade;
  - municipio;
  - regiao;
  - visibilidade.

### Backend

- `GET /api/perfil/me`.
- `PUT /api/perfil/me`.
- `GET /api/perfil/me/experiencias`.
- `POST /api/perfil/me/experiencias`.
- `PATCH /api/perfil/me/experiencias/{id}`.
- `DELETE /api/perfil/me/experiencias/{id}`.

### Frontend

- Criar tela `Meu Perfil`.
- Adicionar item na sidebar.
- Criar formulario principal.
- Criar lista de experiencias.
- Criar estado de perfil incompleto.
- Criar feedback de salvamento.

### Testes

- Testar criacao/edicao de perfil.
- Testar criacao/edicao/remocao de experiencia.
- Testar usuario nao editar perfil de outro.
- Testar persistencia no banco.

### Criterios de aceite

- Usuario salva perfil.
- Usuario cadastra experiencias.
- Perfil persiste no banco.
- Permissoes impedem acesso indevido.

## Fase 3 - Busca Interna de Peritos

Complexidade: media.
Prioridade: alta.
Publico principal: administradores, peritos e futuramente contratantes.

### Objetivo

Transformar os perfis cadastrados em um cadastro pesquisavel dentro do sistema.

### Melhorias

- Criar tela `Buscar Peritos`.
- Filtrar por:
  - especialidade;
  - municipio;
  - regiao;
  - experiencia;
  - papel exercido;
  - perfil publico/interno.
- Visualizar perfil resumido.
- Visualizar perfil completo.
- Mostrar experiencias publicas.
- Respeitar controle de visibilidade.

### Backend

- `GET /api/perfis`.
- `GET /api/perfis/{id}`.
- Parametros de busca por especialidade, regiao, municipio e texto.

### Frontend

- Tela de busca com filtros.
- Cards de peritos.
- Modal ou pagina de detalhes.
- Estado vazio com orientacao.

### Testes

- Busca por especialidade.
- Busca por regiao.
- Perfil privado nao aparece para usuario comum.
- Admin ve todos.

### Criterios de aceite

- Cadastro de peritos fica pesquisavel.
- Visibilidade e permissoes funcionam.
- Busca retorna dados coerentes.

## Fase 4 - CRM Pericial Simples

Complexidade: media.
Prioridade: alta.
Publico principal: peritos e escritorios.

### Objetivo

Permitir acompanhar oportunidades como casos de trabalho, nao apenas como processos na lista.

### Melhorias

- Converter processo acompanhado em oportunidade.
- Criar status da oportunidade.
- Criar responsavel.
- Criar observacoes.
- Criar proxima acao.
- Criar data de follow-up.
- Criar comentarios internos.
- Criar filtros por status/responsavel/prioridade.

### Banco

- Criar `oportunidades_crm`.
- Criar `oportunidade_comentarios`.

### Backend

- CRUD de oportunidades.
- CRUD de comentarios.
- Endpoint para converter processo acompanhado em oportunidade.

### Frontend

- Tela `CRM Pericial`.
- Kanban simples ou tabela operacional.
- Detalhe da oportunidade.
- Campo de comentario.

### Testes

- Criar oportunidade.
- Atualizar status.
- Adicionar comentario.
- Filtrar por responsavel.
- Validar permissoes.

### Criterios de aceite

- Usuario acompanha oportunidade de ponta a ponta.
- Processo relevante pode virar item de CRM.
- Historico de comentario fica persistido.

## Fase 5 - Gestao de Escritorio

Complexidade: media a alta.
Prioridade: media.

### Objetivo

Permitir que escritorios periciais usem o Radar em equipe.

### Melhorias

- Criar organizacoes/escritorios.
- Vincular usuarios a organizacao.
- Criar papel de gestor.
- Distribuir oportunidades para membros.
- Dashboard por equipe.
- Auditoria por organizacao.

### Banco

- Criar `organizacoes`.
- Adicionar `organizacao_id` em `usuarios`.
- Ajustar tabelas futuras para escopo de organizacao.

### Riscos

- Exige revisao de autorizacao em varios endpoints.
- Risco de vazamento entre organizacoes se mal implementado.

### Criterios de aceite

- Dados de uma organizacao nao aparecem para outra.
- Gestor gerencia equipe.
- Admin global audita tudo.

## Fase 6 - Marketplace Restrito

Complexidade: alta.
Prioridade: media.

### Objetivo

Permitir que contratantes encontrem peritos qualificados, inicialmente de forma restrita e controlada.

### Melhorias

- Criar perfil publico/controlado.
- Criar usuario contratante.
- Criar busca de peritos para contratantes.
- Criar solicitacao de contato.
- Registrar interesses.
- Adicionar moderacao basica.

### Cuidados

- Consentimento do perito.
- LGPD.
- Termos de uso.
- Politica de privacidade.
- Protecao contra scraping.

### Criterios de aceite

- Contratante busca peritos.
- Perito controla visibilidade.
- Contatos ficam registrados.

## Fase 7 - Inteligencia Estrategica Regional

Complexidade: alta.
Prioridade: media.

### Objetivo

Gerar inteligencia de mercado a partir dos dados acumulados.

### Melhorias

- Tendencias por regiao.
- Crescimento por tema.
- Comparativo entre comarcas.
- Relatorios mensais.
- Exportacao Excel/PDF.
- Indicadores de mercado pericial.

### Dependencias

- Historico suficiente.
- Coletas confiaveis.
- Deduplicacao forte.
- Classificacao por tema consistente.

### Criterios de aceite

- Relatorio mensal e gerado.
- Indicadores mostram recorte temporal.
- Exportacao funciona.

## Fase 8 - Historico de Vara e Magistrado

Complexidade: alta.
Prioridade: media a baixa.

### Objetivo

Extrair inteligencia institucional: vara, comarca, magistrado e padroes de nomeacao.

### Melhorias

- Historico por vara/comarca.
- Tempo medio ate evento relevante.
- Frequencia de indicios de pericia.
- Historico por magistrado quando os dados permitirem.
- Nivel de confianca por indicador.

### Riscos

- DataJud pode nao fornecer todos os dados estruturados.
- Pode exigir NLP.
- Pode exigir validacao manual de amostras.

### Criterios de aceite

- Indicadores mostram evidencia.
- Sistema informa confianca.
- Nao apresenta inferencia fraca como certeza.

## Fase 9 - Benchmark Nacional

Complexidade: muito alta.
Prioridade: baixa para agora.

### Objetivo

Comparar demanda pericial entre estados e regioes.

### Melhorias

- Coleta multiestado.
- Ranking por UF.
- Comparacao por especialidade.
- Expansao geografica.

### Dependencias

- Infraestrutura maior.
- Limites DataJud revisados.
- Normalizacao nacional.
- Banco maior.

### Criterios de aceite

- Coleta nao estoura limite da API.
- Dados sao comparaveis.
- Relatorios mostram fonte e periodo.

## Fase 10 - Inteligencia Preditiva

Complexidade: muito alta.
Prioridade: baixa para agora.

### Objetivo

Prever tendencias futuras de oportunidades.

### Melhorias

- Previsao por regiao.
- Previsao por especialidade.
- Tendencia de 3 a 6 meses.
- Confianca estatistica.
- Validacao contra historico.

### Dependencias

- Historico limpo.
- Dados suficientes.
- Metricas de erro.
- Validacao continua.

### Criterios de aceite

- Modelo informa confianca.
- Previsao e validada contra dados reais.
- Produto comunica probabilidade, nao certeza.

## Fase 11 - Preparacao de Lancamento

Complexidade: alta.
Prioridade: somente depois das fases locais essenciais.

### Pre-condicoes

- Fases 0 a 4 aprovadas localmente.
- Coleta DataJud auditada com amostras reais.
- Testes automatizados passando.
- Docker build limpo.
- Perfis e CRM validados.
- Fluxos principais testados por usuario real.

### Melhorias

- Subir para GitHub.
- Configurar CI em pull request.
- Configurar branch protection.
- Configurar secrets.
- Criar homologacao online.
- Configurar dominio e HTTPS.
- Configurar PostgreSQL/PostGIS gerenciado.
- Configurar Redis gerenciado.
- Criar backup diario.
- Testar restore.
- Rodar smoke test autenticado.
- Criar termos de uso.
- Criar politica de privacidade.
- Revisar juridicamente DataJud/CNJ e demais fontes.

### Criterios de aceite

- Homologacao funciona igual ao local.
- CI passa.
- Smoke test passa.
- Backup/restore validado.
- Segredos fora do Git.
- `DEFAULT_ADMIN_PASSWORD` removido apos bootstrap.

## Trilhas Tecnicas Permanentes

### Banco e migracoes

- Migrar gradualmente schema de `database/db.py` para `database/migrations`.
- Manter SQL versionado simples por enquanto.
- Reavaliar Alembic somente se houver muitos ambientes, conflitos ou necessidade de downgrade.
- Criar backup antes de migracoes em homologacao/producao.

### Testes

- Manter testes de API com `TestClient`.
- Ampliar testes de banco.
- Manter teste PostGIS no CI.
- Criar testes para cada endpoint novo.
- Criar smoke test local e homologacao.
- Futuramente adicionar teste visual com Playwright.

### Dados e DataJud

- Manter coleta conservadora.
- Registrar metricas por classe.
- Auditar amostras de CNJ.
- Monitorar 401, 429 e timeout.
- Medir descartes por falta de CNJ.
- Medir duplicados.
- Validar mapeamento de municipio/comarca.

### Segurança

- Manter hash forte de senha.
- Manter sessoes com token seguro.
- Manter rate limit.
- Remover senha admin padrao apos bootstrap.
- Rotacionar `SECRET_KEY` e `SESSION_TOKEN_PEPPER` em homologacao antes da producao.
- Evitar logs de segredo.
- Revisar LGPD antes de marketplace.

### Frontend

- Manter React/Vite/TypeScript.
- Melhorar responsividade.
- Melhorar estados vazios.
- Melhorar microcopy.
- Evitar tela sem backend real.
- Validar em desktop, notebook e mobile.

## Ordem Pratica Recomendada

1. Fechar validacao local da Fase 0.
2. Implementar ranking e filtros por tema da Fase 1.
3. Implementar perfil profissional da Fase 2.
4. Implementar busca interna da Fase 3.
5. Implementar CRM simples da Fase 4.
6. Revisar produto com usuario real.
7. So depois iniciar homologacao online.

## Definicao de Pronto para Lançamento

O projeto so deve ir para lancamento quando:

- os fluxos locais essenciais estiverem funcionando;
- coleta real estiver auditada;
- login/permissoes estiverem consistentes;
- perfil profissional estiver funcional;
- busca interna estiver funcional;
- CRM simples estiver funcional;
- CI passar;
- homologacao passar smoke test;
- backup e restore forem testados;
- juridico/compliance das fontes estiver revisado.
