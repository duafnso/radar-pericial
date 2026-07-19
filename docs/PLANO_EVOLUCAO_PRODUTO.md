# Plano de Evolucao do Radar Pericial por Fases

Estado: planejamento atualizado para desenvolvimento local.

Documento complementar: `docs/PLANO_MESTRE_MELHORIAS.md` consolida todas as melhorias propostas, incluindo trilhas tecnicas permanentes, dados, seguranca, testes, frontend e lancamento.
Premissa principal: o produto continua rodando localmente para testes. A etapa de lancamento, hospedagem publica, venda comercial e operacao em producao so deve comecar depois que as fases essenciais estiverem funcionais, testadas e aprovadas.

## Visao de Produto

O Radar Pericial deve evoluir de um radar de oportunidades judiciais para uma plataforma completa de inteligencia, presenca profissional, gestao pericial e marketplace.

A visao final contempla cinco grupos de servico:

1. Inteligencia de Presenca: mostra onde o perito deve investir presenca, quais comarcas/varas estao aquecidas e quais processos possuem maior chance de pericia.
2. Perfil Profissional: perfil estilo LinkedIn para peritos e assistentes tecnicos, com curriculo pericial vivo, especialidades e historico de atuacao.
3. Inteligencia Estrategica: analises regionais, tendencias, relatorios e comparativos de demanda pericial.
4. Gestao de Escritorio: CRM pericial, equipe, distribuicao de oportunidades e acompanhamento interno.
5. Acesso ao Cadastro: marketplace para advogados, sindicatos, empresas e instituicoes encontrarem peritos qualificados.

## Regra de Ordem

A ordem abaixo prioriza menor complexidade primeiro, criando valor util antes de tentar funcionalidades caras ou incertas.

Criterios usados para ordenar:

- baixo risco tecnico;
- aproveitamento do que ja existe;
- utilidade imediata para teste local;
- dependencia de dados reais disponiveis;
- impacto comercial futuro;
- menor necessidade de infraestrutura externa.

## Fase 0 - Consolidacao Local do MVP Atual

Complexidade: baixa.
Objetivo: garantir que o sistema atual esteja limpo, estavel e coerente antes de adicionar novos modulos.

### Entregas

- Corrigir textos quebrados, encoding e mensagens visuais.
- Manter logo, identidade visual e navegacao principal consistentes.
- Validar login, dashboard, processos, mapa, coletas, usuarios e auditoria.
- Garantir que `docker compose up -d --build` funcione em maquina local.
- Garantir que `npm run frontend:build` continue passando.
- Garantir que testes backend continuem passando.
- Revisar se a coleta DataJud esta salvando dados reais e metricas corretas.
- Conferir se Radar de Processos mostra dados coletados de 2026 em diante quando configurado.

### Criterios de aceite

- Sistema sobe localmente sem erro.
- Interface nao mostra textos quebrados.
- Coletas mostram status claro.
- Radar de Processos abre detalhes do processo.
- Acompanhamento de processo gera alerta.
- Usuarios e permissoes funcionam.
- Health checks passam.

## Fase 1 - Inteligencia de Presenca Essencial

Complexidade: baixa a media.
Objetivo: transformar o Radar atual em uma ferramenta mais direta para responder: onde o perito deve atuar agora?

### Funcionalidades

- Ranking semanal de oportunidades.
- Top 10 processos com maior chance de pericia.
- Filtros por tema:
  - desapropriacao;
  - servidao;
  - ambiental;
  - seguro agricola;
  - usucapiao;
  - avaliacao rural.
- Filtros por comarca, municipio, regiao e classe processual.
- Mapa de presenca por municipio/comarca com pins de processos.
- Resumo de areas aquecidas no dashboard.
- Alertas por tema e regiao.
- Indicador de ultima coleta DataJud usada nos dados.

### Dados necessarios

- Processos DataJud ja coletados.
- Classe processual.
- Assunto.
- Municipio/comarca.
- Score pericial.
- Data de distribuicao.
- Movimentacoes quando disponiveis.

### Criterios de aceite

- Usuario consegue abrir o sistema e enxergar as melhores oportunidades sem analisar centenas de processos.
- Dashboard mostra ranking e areas aquecidas.
- Radar permite filtrar oportunidades por tema e regiao.
- Alertas funcionam para processos acompanhados e filtros basicos.

## Fase 2 - Perfil Profissional do Perito

Complexidade: media.
Objetivo: criar a peca central do produto: o perfil profissional que o perito alimenta e que futuramente sera consumido por contratantes.

### Funcionalidades

- Tela `Meu Perfil Profissional`.
- Cadastro de dados profissionais:
  - nome completo;
  - titulo profissional;
  - registro profissional, se aplicavel;
  - telefone/email profissional;
  - cidade base;
  - regioes de atuacao;
  - mini bio.
- Cadastro de especialidades:
  - pericia agronomica;
  - avaliacao rural;
  - ambiental;
  - georreferenciamento;
  - produtividade agricola;
  - desapropriacao;
  - servidao;
  - seguro agricola;
  - assistencia tecnica judicial.
- Cadastro de experiencias periciais:
  - tipo de pericia;
  - comarca/municipio;
  - ano;
  - area aproximada;
  - papel exercido: perito, assistente tecnico, consultor;
  - observacao resumida;
  - visibilidade publica ou privada.
- Upload futuro de documentos comprobatórios deve ficar para fase posterior, nao para MVP local.

### Mudancas tecnicas

- Criar tabelas:
  - `perfis_profissionais`;
  - `perfil_especialidades`;
  - `experiencias_periciais`.
- Criar endpoints autenticados:
  - `GET /api/perfil/me`;
  - `PUT /api/perfil/me`;
  - `GET /api/perfil/me/experiencias`;
  - `POST /api/perfil/me/experiencias`;
  - `PATCH /api/perfil/me/experiencias/{id}`;
  - `DELETE /api/perfil/me/experiencias/{id}`.
- Criar tela React de perfil.
- Adicionar item na sidebar.

### Criterios de aceite

- Usuario logado consegue preencher e salvar seu perfil.
- Usuario consegue cadastrar experiencias periciais.
- Perfil cresce como um curriculo vivo.
- Dados ficam persistidos no banco local.
- Permissoes impedem edicao de perfil de outro usuario comum.

## Fase 3 - Busca Interna de Peritos

Complexidade: media.
Objetivo: criar a primeira versao do cadastro pesquisavel, ainda local e restrito ao sistema.

### Funcionalidades

- Tela `Buscar Peritos`.
- Lista filtravel por:
  - especialidade;
  - municipio;
  - regiao;
  - tipo de atuacao;
  - experiencia cadastrada;
  - disponibilidade futura, se adicionada.
- Visualizacao de perfil resumido.
- Visualizacao de perfil completo.
- Controle de visibilidade:
  - perfil privado;
  - perfil visivel apenas para usuarios logados;
  - perfil elegivel para marketplace futuro.

### Criterios de aceite

- Admin consegue visualizar todos os perfis.
- Usuario comum consegue visualizar apenas perfis liberados.
- Filtros retornam resultados coerentes.
- Perfil profissional vira ativo pesquisavel dentro do sistema.

## Fase 4 - CRM Pericial Simples

Complexidade: media.
Objetivo: permitir que peritos e escritorios acompanhem oportunidades como casos de trabalho.

### Funcionalidades

- Converter processo acompanhado em oportunidade de CRM.
- Campos basicos:
  - responsavel;
  - status;
  - prioridade;
  - observacoes;
  - proxima acao;
  - data de follow-up;
  - origem: DataJud, manual, administrativo.
- Status sugeridos:
  - novo;
  - em analise;
  - contato iniciado;
  - proposta enviada;
  - nomeado/contratado;
  - perdido;
  - arquivado.
- Comentarios internos por oportunidade.
- Filtro por responsavel e status.

### Mudancas tecnicas

- Criar tabelas:
  - `oportunidades_crm`;
  - `oportunidade_comentarios`.
- Criar endpoints CRUD.
- Criar tela React `CRM Pericial`.
- Permissoes por perfil:
  - usuario ve suas oportunidades;
  - operator/admin ve oportunidades da equipe.

### Criterios de aceite

- Usuario consegue transformar processo em oportunidade.
- Usuario consegue acompanhar status e observacoes.
- Escritorio consegue distribuir oportunidade para responsavel.

## Fase 5 - Gestao de Escritorio

Complexidade: media a alta.
Objetivo: permitir uso por equipes, escritorios periciais e empresas de assistencia tecnica.

### Funcionalidades

- Criar conceito de organizacao/escritorio.
- Usuarios pertencem a uma organizacao.
- Gestor visualiza equipe.
- Gestor distribui oportunidades.
- Dashboard por equipe:
  - oportunidades abertas;
  - oportunidades por responsavel;
  - processos acompanhados;
  - conversao de oportunidades;
  - produtividade por periodo.

### Mudancas tecnicas

- Criar tabela `organizacoes`.
- Vincular `usuarios` a `organizacao_id`.
- Ajustar permissoes para escopo de organizacao.
- Revisar auditoria para registrar organizacao.
- Revisar endpoints para evitar vazamento entre organizacoes.

### Criterios de aceite

- Um escritorio consegue operar com varios usuarios.
- Dados de uma organizacao nao aparecem para outra.
- Admin global continua podendo auditar tudo localmente.

## Fase 6 - Marketplace Restrito de Peritos

Complexidade: alta.
Objetivo: preparar o lado dos contratantes, ainda antes do lancamento publico.

### Funcionalidades

- Perfil publico/controlado do perito.
- Busca por peritos para usuarios contratantes.
- Tipos de usuario novos:
  - contratante;
  - institucional;
  - escritorio;
  - perito.
- Botao de contato ou solicitacao de contato.
- Registro de interesse:
  - quem viu;
  - quem solicitou contato;
  - qual especialidade/regiao buscou.
- Moderacao basica de perfil.

### Pontos de cuidado

- LGPD.
- Consentimento de exposicao do perfil.
- Dados de contato publicos ou protegidos.
- Regras contra raspagem/uso abusivo.
- Termos de uso.

### Criterios de aceite

- Contratante consegue buscar perfil liberado.
- Perito controla visibilidade do perfil.
- Solicitudes ficam registradas.
- Nao ha exposicao indevida de dados pessoais.

## Fase 7 - Inteligencia Estrategica Regional

Complexidade: alta.
Objetivo: transformar dados acumulados em inteligencia de mercado.

### Funcionalidades

- Tendencia por regiao.
- Crescimento de temas por periodo.
- Comparativo entre comarcas.
- Relatorio mensal automatico.
- Exportacao Excel/PDF.
- Indicadores:
  - processos por tema;
  - crescimento mensal;
  - municipios aquecidos;
  - oportunidades por especialidade;
  - taxa de processos com indicio de pericia.

### Dependencias

- Base de dados historica suficiente.
- Coletas confiaveis por varios periodos.
- Deduplicacao forte.
- Classificacao por tema consistente.

### Criterios de aceite

- Relatorio mensal gera indicadores coerentes.
- Usuario consegue exportar dados.
- Dashboard estrategico nao depende de calculos manuais.

## Fase 8 - Historico de Vara e Magistrado

Complexidade: alta.
Objetivo: extrair inteligencia institucional mais profunda a partir dos processos.

### Funcionalidades desejadas

- Historico por vara/comarca:
  - volume de processos por tema;
  - processos com indicio de pericia;
  - tempo medio ate nomeacao;
  - frequencia de movimentos relevantes.
- Historico por magistrado, se os dados permitirem:
  - padrao de nomeacao;
  - indicios de pericia externa;
  - exigencia de vistoria;
  - abertura para assistente tecnico.

### Risco tecnico

Esta fase depende de qualidade e disponibilidade dos textos de movimentacao/processo. Pode ser limitada pela API, por ausencia de dados estruturados ou por necessidade de NLP/classificacao textual.

### Criterios de aceite

- O sistema deve informar nivel de confianca do indicador.
- Indicadores devem ser baseados em evidencias rastreaveis.
- Nao exibir conclusoes fortes quando os dados forem insuficientes.

## Fase 9 - Benchmark Nacional

Complexidade: muito alta.
Objetivo: comparar demanda pericial entre estados e regioes.

### Funcionalidades

- Coleta multiestado.
- Comparacao por UF.
- Comparacao por especialidade.
- Ranking nacional de demanda.
- Expansao de mapas e regioes alem de Mato Grosso.

### Dependencias

- Aumento de escala de coleta DataJud.
- Controle rigoroso de limite de API.
- Banco maior.
- Infraestrutura mais robusta.
- Normalizacao de municipios, tribunais e classes em nivel nacional.

### Criterios de aceite

- Coleta nacional nao quebra limites da API.
- Dados sao comparaveis entre UFs.
- Relatorios indicam recorte temporal e fonte.

## Fase 10 - Inteligencia Preditiva

Complexidade: muito alta.
Objetivo: estimar onde oportunidades futuras devem surgir.

### Funcionalidades

- Previsao de demanda por regiao e especialidade.
- Tendencia para 3 a 6 meses.
- Indicadores com confianca estatistica.
- Simulacoes por tema/regiao.

### Requisitos minimos

- Historico consistente.
- Dados limpos.
- Periodo minimo observavel.
- Metricas de erro.
- Validacao contra periodos passados.

### Criterios de aceite

- Modelo deve mostrar confianca e limitacoes.
- Previsoes devem ser comparadas com dados reais posteriores.
- Nao vender como certeza; vender como inteligencia probabilistica.

## Fase 11 - Preparacao de Lancamento

Complexidade: alta, mas so depois das fases locais essenciais.
Objetivo: sair do ambiente local para homologacao e producao comercial.

### Pre-condicoes

- Fases 0 a 4 funcionando localmente.
- Perfil profissional validado.
- Busca interna validada.
- CRM simples validado.
- Coleta DataJud auditada com amostras reais.
- Testes automatizados passando.
- Docker build limpo.

### Entregas

- GitHub remoto.
- CI rodando em pull request.
- Branch protection.
- Secrets configurados.
- Ambiente de homologacao.
- Dominio.
- HTTPS.
- PostgreSQL/PostGIS gerenciado.
- Redis gerenciado.
- Backups.
- Smoke test autenticado.
- Termos de uso.
- Politica de privacidade.
- Revisao juridica das fontes.

### Criterios de aceite

- Homologacao roda igual ao ambiente local.
- Smoke test passa.
- Backup e restore testados.
- Segredos fora do Git.
- `DEFAULT_ADMIN_PASSWORD` removido apos bootstrap.
- Politica comercial e compliance definidos.

## Roadmap Recomendado Agora

A proxima sequencia pratica deve ser:

1. Fechar Fase 0: consolidacao local e validacao dos fluxos atuais.
2. Implementar Fase 1: ranking semanal, areas aquecidas e filtros por tema.
3. Implementar Fase 2: perfil profissional do perito.
4. Implementar Fase 3: busca interna de peritos.
5. Implementar Fase 4: CRM pericial simples.
6. Somente depois avaliar homologacao e lancamento.

## Decisao Estrategica

O Grupo 2, Perfil Profissional, deve ser construido cedo. Ele e o ativo central da plataforma:

- o perito alimenta o proprio curriculo;
- o Radar ajuda o perito a encontrar oportunidades;
- as experiencias aumentam o valor do perfil;
- contratantes futuramente consomem esse cadastro;
- o marketplace nasce em cima de dados que os proprios profissionais mantem.

Assim, o produto deixa de ser apenas uma ferramenta de consulta e passa a criar uma base proprietaria de reputacao pericial.