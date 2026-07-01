from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    KeepTogether,
    ListFlowable,
    ListItem,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "docs"
PDF_PATH = OUT_DIR / "Radar_Pericial_Plano_Comercial_e_Tecnico.pdf"


TITLE = "Radar Pericial"
SUBTITLE = "Plano Comercial, Arquitetura Tecnica e Roteiro de Producao"


def _styles():
    base = getSampleStyleSheet()
    styles = {
        "cover_title": ParagraphStyle(
            "cover_title",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=30,
            leading=36,
            textColor=colors.HexColor("#143B2F"),
            alignment=TA_CENTER,
            spaceAfter=12,
        ),
        "cover_subtitle": ParagraphStyle(
            "cover_subtitle",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=13,
            leading=18,
            textColor=colors.HexColor("#46524E"),
            alignment=TA_CENTER,
            spaceAfter=18,
        ),
        "meta": ParagraphStyle(
            "meta",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=9,
            leading=13,
            textColor=colors.HexColor("#6B7280"),
            alignment=TA_CENTER,
        ),
        "h1": ParagraphStyle(
            "h1",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=23,
            textColor=colors.HexColor("#143B2F"),
            spaceBefore=10,
            spaceAfter=8,
        ),
        "h2": ParagraphStyle(
            "h2",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=17,
            textColor=colors.HexColor("#245947"),
            spaceBefore=8,
            spaceAfter=5,
        ),
        "h3": ParagraphStyle(
            "h3",
            parent=base["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=14,
            textColor=colors.HexColor("#2F4F46"),
            spaceBefore=6,
            spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "body",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9.6,
            leading=13.2,
            textColor=colors.HexColor("#202624"),
            spaceAfter=6,
            alignment=TA_LEFT,
        ),
        "small": ParagraphStyle(
            "small",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=8.2,
            leading=11,
            textColor=colors.HexColor("#374151"),
            spaceAfter=4,
        ),
        "table_head": ParagraphStyle(
            "table_head",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8.4,
            leading=10,
            textColor=colors.white,
        ),
        "table_cell": ParagraphStyle(
            "table_cell",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=7.8,
            leading=10,
            textColor=colors.HexColor("#202624"),
        ),
        "callout": ParagraphStyle(
            "callout",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9,
            leading=12.5,
            textColor=colors.HexColor("#1F2937"),
            leftIndent=0,
            rightIndent=0,
            spaceAfter=0,
        ),
        "toc": ParagraphStyle(
            "toc",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9,
            leading=13,
            textColor=colors.HexColor("#1F2937"),
            spaceAfter=4,
        ),
    }
    return styles


def p(text, style):
    return Paragraph(text, style)


def bullet_list(items, styles):
    return ListFlowable(
        [ListItem(p(item, styles["body"]), leftIndent=12) for item in items],
        bulletType="bullet",
        start="circle",
        leftIndent=18,
        bulletFontName="Helvetica",
        bulletFontSize=7,
        spaceAfter=6,
    )


def numbered_list(items, styles):
    return ListFlowable(
        [ListItem(p(item, styles["body"]), leftIndent=14) for item in items],
        bulletType="1",
        leftIndent=18,
        bulletFontName="Helvetica",
        bulletFontSize=8,
        spaceAfter=6,
    )


def table(data, widths, styles, header=True):
    prepared = []
    for ridx, row in enumerate(data):
        row_style = styles["table_head"] if header and ridx == 0 else styles["table_cell"]
        prepared.append([p(str(cell), row_style) for cell in row])
    t = Table(prepared, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#245947") if header else colors.white),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white if header else colors.HexColor("#202624")),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#CBD5D1")),
                ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#9FB5AE")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FAF8")]),
            ]
        )
    )
    return t


def callout(title, text, styles, fill="#F1F7F4", border="#7BA494"):
    data = [[p(f"<b>{title}</b><br/>{text}", styles["callout"])]]
    t = Table(data, colWidths=[17.2 * cm], hAlign="LEFT")
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(fill)),
                ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor(border)),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    return KeepTogether([t, Spacer(1, 8)])


def header_footer(canvas, doc):
    canvas.saveState()
    width, height = A4
    canvas.setStrokeColor(colors.HexColor("#CBD5D1"))
    canvas.setLineWidth(0.4)
    canvas.line(1.8 * cm, height - 1.35 * cm, width - 1.8 * cm, height - 1.35 * cm)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#6B7280"))
    canvas.drawString(1.8 * cm, height - 1.1 * cm, "Radar Pericial - Plano Comercial e Tecnico")
    canvas.drawRightString(width - 1.8 * cm, 1.15 * cm, f"Pagina {doc.page}")
    canvas.restoreState()


def cover_page(canvas, doc):
    canvas.saveState()
    width, height = A4
    canvas.setFillColor(colors.HexColor("#F6FAF7"))
    canvas.rect(0, 0, width, height, fill=True, stroke=False)
    canvas.setFillColor(colors.HexColor("#143B2F"))
    canvas.rect(0, height - 2.2 * cm, width, 2.2 * cm, fill=True, stroke=False)
    canvas.setFillColor(colors.HexColor("#D7E7DE"))
    canvas.rect(0, 0, width, 1.1 * cm, fill=True, stroke=False)
    canvas.restoreState()


def add_section(story, styles, title, intro=None):
    story.append(p(title, styles["h1"]))
    if intro:
        story.append(p(intro, styles["body"]))


def build_story(styles):
    story = []

    story.append(Spacer(1, 4.2 * cm))
    story.append(p(TITLE, styles["cover_title"]))
    story.append(p(SUBTITLE, styles["cover_subtitle"]))
    story.append(Spacer(1, 0.6 * cm))
    story.append(
        callout(
            "Documento executivo",
            "Roteiro minucioso para transformar o Radar Pericial em uma aplicacao online, hospedada com Docker, operada de forma confiavel e preparada para venda comercial.",
            styles,
            fill="#FFFFFF",
            border="#9FB5AE",
        )
    )
    story.append(Spacer(1, 1.5 * cm))
    story.append(p("Versao: 1.0 | Projeto: Radar Pericial | Publico-alvo: operacao, produto, tecnologia e comercial", styles["meta"]))
    story.append(NextPageTemplate("normal"))
    story.append(PageBreak())

    story.append(p("Sumario Executivo", styles["h1"]))
    toc_items = [
        "1. Visao geral do produto",
        "2. Stack oficial atual e stack recomendada",
        "3. Funcionamento oficial da aplicacao",
        "4. Arquitetura de producao com Docker",
        "5. Fases de implantacao comercial",
        "6. Seguranca, LGPD e compliance",
        "7. Banco de dados, multi-tenant e billing",
        "8. Observabilidade, testes e operacao",
        "9. Roadmap, checklist e criterios de pronto",
    ]
    for item in toc_items:
        story.append(p(item, styles["toc"]))
    story.append(
        callout(
            "Conclusao curta",
            "O Radar Pericial tem base tecnica promissora, mas deve passar por hardening, separacao de ambientes, controle de segredos, modelo multi-cliente, observabilidade e processo comercial antes de ser vendido como SaaS.",
            styles,
        )
    )
    story.append(PageBreak())

    add_section(
        story,
        styles,
        "1. Visao Geral do Produto",
        "O Radar Pericial e uma plataforma de inteligencia judicial, administrativa e geoespacial para identificar oportunidades de pericia agronomica e fundiaria, inicialmente com foco no Mato Grosso.",
    )
    story.append(p("A aplicacao cruza processos judiciais, eventos administrativos e dados geoespaciais para sugerir onde ha maior probabilidade de demanda por avaliacao rural, pericia agronomica, vistoria fundiaria, desapropriacao, servidao administrativa, conflitos possessórios, dano ambiental e temas correlatos.", styles["body"]))
    story.append(p("Publicos comerciais prioritarios", styles["h2"]))
    story.append(bullet_list([
        "Peritos agronomos que desejam identificar oportunidades antes da nomeacao formal.",
        "Escritorios de engenharia agronomica, avaliacao rural e assistencia tecnica judicial.",
        "Advogados agraristas, ambientalistas e imobiliarios que atuam com imoveis rurais.",
        "Empresas rurais e consultorias fundiarias que precisam monitorar riscos territoriais.",
        "Associacoes, sindicatos e entidades setoriais interessadas em inteligencia territorial.",
    ], styles))
    story.append(p("Proposta de valor", styles["h2"]))
    story.append(bullet_list([
        "Reduzir tempo de prospeccao manual em diarios oficiais, DataJud, portais administrativos e bases geoespaciais.",
        "Centralizar dados dispersos em um painel unico, com mapa e score de oportunidade.",
        "Gerar alertas acionaveis por municipio, regiao, classe processual, fonte e faixa de score.",
        "Transformar dados publicos em inteligencia comercial para atuacao pericial.",
    ], styles))

    story.append(p("2. Stack Oficial Atual", styles["h1"]))
    story.append(table([
        ["Camada", "Tecnologias atuais", "Funcao"],
        ["Backend", "Python 3.11, FastAPI, Pydantic", "API REST, autenticacao, healthchecks, servico do frontend e endpoints do painel."],
        ["Banco", "PostgreSQL, PostGIS, SQLAlchemy", "Persistencia relacional, geometrias, consultas espaciais, indices GIST e dados normalizados."],
        ["Processamento", "Celery, Celery Beat, Redis", "Filas, agendamento de coletas, workers judicial/admin/geo e alertas."],
        ["Geoespacial", "GeoPandas, Shapely, Fiona, PyProj", "ETL, CRS, validacao de geometria, clip territorial e enriquecimento municipal."],
        ["Coleta", "Requests, BeautifulSoup", "Consumo de APIs publicas, scraping controlado e fallback de fontes externas."],
        ["Frontend", "HTML, CSS, JavaScript vanilla, Leaflet", "Dashboard, mapas, filtros, calculadora de score e cadastro de peritos."],
        ["Infra local", "Docker, Docker Compose", "Ambiente reprodutivel com web, worker, beat, PostGIS e Redis."],
    ], [3.0 * cm, 5.1 * cm, 9.1 * cm], styles))
    story.append(p("Stack recomendada para producao comercial", styles["h2"]))
    story.append(bullet_list([
        "Manter Docker para empacotar web, worker e beat.",
        "Usar banco PostGIS gerenciado quando possivel, evitando manutencao manual de backup, disco e atualizacoes.",
        "Usar Redis gerenciado para reduzir risco operacional.",
        "Separar ambiente local, staging e production.",
        "Adicionar Alembic para migrations formais.",
        "Adicionar pytest para testes automatizados.",
        "Adicionar Sentry ou ferramenta equivalente para erros e observabilidade.",
    ], styles))

    story.append(p("3. Funcionamento Oficial da Aplicacao", styles["h1"]))
    story.append(numbered_list([
        "Usuario acessa o dominio oficial da plataforma via HTTPS.",
        "Frontend carrega a interface e verifica se existe token de sessao valido.",
        "Se nao houver sessao, o usuario informa credenciais na tela de login.",
        "FastAPI valida usuario e senha no PostgreSQL usando hash seguro.",
        "Backend cria token de sessao, armazena apenas o hash do token e retorna o token ao frontend.",
        "Frontend envia chamadas posteriores com Authorization: Bearer token.",
        "Endpoints protegidos validam sessao, expiracao e revogacao.",
        "Dashboard consulta estatisticas, processos, eventos, alertas, peritos e GeoJSON.",
        "Celery Beat agenda coletas periodicas conforme fonte e prioridade.",
        "Workers executam coletas, normalizam payloads, chamam ETL e persistem dados.",
        "Motor de score classifica processos e eventos por probabilidade de oportunidade pericial.",
        "Alertas sao registrados e entregues por interface, Telegram, webhook ou e-mail.",
        "Administradores acompanham status de fontes, erros de coleta, usuarios, planos e assinaturas.",
    ], styles))
    story.append(callout("Regra de produto", "O usuario final nao deve precisar entender DataJud, PostGIS, Celery ou fontes externas. Ele deve enxergar oportunidades, mapas, alertas, filtros e relatorios.", styles))

    story.append(p("4. Arquitetura de Producao com Docker", styles["h1"]))
    story.append(p("O docker-compose atual deve continuar existindo para desenvolvimento. Em producao, a arquitetura deve separar os processos em servicos independentes: web, worker, beat, banco e Redis.", styles["body"]))
    story.append(table([
        ["Servico", "Responsabilidade", "Escala"],
        ["web", "FastAPI, endpoints REST, login, frontend e healthchecks.", "Escala horizontal por CPU/requisicoes."],
        ["worker", "Coletas, ETL, scoring em lote e envio de alertas.", "Escala por fila e volume de tarefas."],
        ["beat", "Agenda tarefas periodicas.", "Normalmente uma unica instancia ativa."],
        ["postgres/postgis", "Banco principal e consultas espaciais.", "Escala vertical inicialmente; backups obrigatorios."],
        ["redis", "Broker Celery e backend de resultados temporarios.", "Gerenciado ou dedicado; monitorar memoria."],
    ], [3.0 * cm, 9.0 * cm, 5.2 * cm], styles))
    story.append(p("Topologia recomendada", styles["h2"]))
    story.append(bullet_list([
        "Dominio oficial apontando para o servico web.",
        "HTTPS obrigatorio em todos os ambientes publicos.",
        "Banco acessivel apenas por rede privada ou allowlist.",
        "Redis sem exposicao publica.",
        "Workers sem porta HTTP publica.",
        "Beat executado como processo unico para evitar duplicidade de agendamento.",
        "Variaveis de ambiente configuradas no painel da plataforma, nunca commitadas no Git.",
    ], styles))

    story.append(p("5. Fases de Implantacao Comercial", styles["h1"]))
    story.append(table([
        ["Fase", "Objetivo", "Entregaveis"],
        ["0 - Arrumacao", "Eliminar riscos basicos do repositorio.", ".gitignore, .env.example, segredos removidos, chaves rotacionadas, healthcheck corrigido."],
        ["1 - Hardening", "Tornar login, API e configuracao aptos a ambiente publico.", "CORS restrito, SECRET_KEY obrigatoria, rate limit, sessoes robustas, logs sem segredo."],
        ["2 - Infra staging", "Criar ambiente online nao comercial para testes reais.", "Deploy web/worker/beat, PostGIS, Redis, variaveis, dominio staging."],
        ["3 - Dados e coletas", "Validar fontes reais e registrar saude operacional.", "Tabela de execucoes, status por fonte, retries, alertas de falha."],
        ["4 - Produto piloto", "Entregar uso real para poucos clientes acompanhados.", "Dashboard, alertas, exportacao basica, suporte manual, onboarding."],
        ["5 - Comercial", "Transformar piloto em SaaS vendavel.", "Planos, contratos, billing, tenant, termos, politica de privacidade."],
        ["6 - Escala", "Preparar crescimento de usuarios e volume de dados.", "Cache GeoJSON, filas dedicadas, autoscaling, auditoria, relatorios."],
    ], [2.4 * cm, 5.1 * cm, 9.7 * cm], styles))

    story.append(p("6. Seguranca, Segredos e LGPD", styles["h1"]))
    story.append(p("Para vender comercialmente, seguranca deixa de ser detalhe tecnico e passa a ser requisito de produto. O sistema lida com dados publicos, mas tambem com usuarios, preferencias, alertas, pesquisas e possiveis dados profissionais cadastrados.", styles["body"]))
    story.append(p("Checklist de seguranca minima", styles["h2"]))
    story.append(bullet_list([
        "Remover .env e .env.txt do versionamento.",
        "Rotacionar DATAJUD_API_KEY e qualquer token exposto.",
        "Exigir SECRET_KEY forte em producao.",
        "Exigir SESSION_TOKEN_PEPPER forte em producao.",
        "Bloquear fallback inseguro para pepper de desenvolvimento.",
        "Configurar CORS apenas para dominios oficiais.",
        "Ativar HTTPS e redirecionamento automatico.",
        "Adicionar rate limit em login e endpoints sensiveis.",
        "Registrar auditoria de login, logout, criacao de perito, alteracao de configuracao e exportacoes.",
        "Nunca logar senha, token, API key ou payload sensivel.",
    ], styles))
    story.append(p("LGPD e governanca de dados", styles["h2"]))
    story.append(bullet_list([
        "Formalizar politica de privacidade e termos de uso.",
        "Explicar fontes publicas utilizadas e finalidade do tratamento.",
        "Remover CPF, CNPJ, nome de proprietario, telefone, e-mail e documentos sempre que nao forem essenciais.",
        "Separar dados publicos globais de dados privados do cliente.",
        "Permitir exclusao ou desativacao de usuarios.",
        "Manter logs de acesso e trilha de auditoria.",
        "Criar procedimento de resposta a incidente.",
    ], styles))

    story.append(p("7. Banco de Dados, Migrations e Multi-Tenant", styles["h1"]))
    story.append(p("A criacao de schema diretamente no codigo e aceitavel para prototipo, mas em producao deve ser substituida gradualmente por migrations versionadas com Alembic.", styles["body"]))
    story.append(table([
        ["Grupo", "Tabelas recomendadas"],
        ["Comercial", "tenants, planos, assinaturas, limites_plano, faturas"],
        ["Usuarios", "usuarios, user_sessions, password_resets, auditoria"],
        ["Configuracao", "tenant_config, fontes_habilitadas, regioes_monitoradas, municipios_monitorados"],
        ["Operacao", "fontes_coleta, execucoes_coleta, erros_coleta, tarefas_agendadas"],
        ["Negocio", "processos, movimentacoes, publicacoes, score_pericial, portarias_diario_oficial"],
        ["Geoespacial", "municipios_mt, parcelas_sigef, assentamentos_incra, inpe_prodes, inpe_deter, cadastro_ambiental"],
        ["Alertas", "alertas, alerta_entregas, canais_alerta, webhooks"],
    ], [4.0 * cm, 13.2 * cm], styles))
    story.append(p("Modelo multi-tenant recomendado", styles["h2"]))
    story.append(bullet_list([
        "Dados publicos e camadas geoespaciais podem ser globais.",
        "Usuarios, configuracoes, alertas, favoritos, relatorios e peritos devem ter tenant_id.",
        "Aplicar tenant_id em todas as consultas de dados privados.",
        "Criar testes para impedir vazamento entre tenants.",
        "Para clientes enterprise, considerar schema ou banco dedicado.",
    ], styles))

    story.append(p("8. Produto, UX e Modulos Oficiais", styles["h1"]))
    story.append(table([
        ["Modulo", "Funcao oficial"],
        ["Dashboard", "Visao executiva: totais, janelas quentes, processos provaveis, eventos recentes e status geral."],
        ["Processos", "Pesquisa e filtro de processos por classe, municipio, regiao, faixa de score e origem."],
        ["Administrativo", "Monitoramento de DOU, DNIT, SINFRA, IOMAT e demais fontes administrativas."],
        ["Mapa", "Visualizacao Leaflet de municipios, parcelas, assentamentos, PRODES, DETER e areas relevantes."],
        ["Score", "Calculadora manual para avaliar texto, classe processual, assunto e movimentacoes."],
        ["Peritos", "Cadastro e gestao de profissionais, especialidades, regioes e score profissional."],
        ["Alertas", "Linha do tempo de eventos relevantes e entregas por canal."],
        ["Admin", "Gestao de usuarios, tenants, planos, fontes, coletas e erros operacionais."],
    ], [3.4 * cm, 13.8 * cm], styles))
    story.append(p("Melhorias prioritarias de UX", styles["h2"]))
    story.append(bullet_list([
        "Estado claro de carregamento, erro e sessao expirada.",
        "Tela de login com recuperacao de senha.",
        "Painel de configuracao de fontes e regioes monitoradas.",
        "Exportacao CSV e PDF.",
        "Filtros persistentes por usuario.",
        "Indicacao de ultima coleta por fonte.",
        "Mensagens que expliquem ausencia de dados sem expor erro tecnico.",
    ], styles))

    story.append(p("9. Coletas, ETL e Qualidade de Dados", styles["h1"]))
    story.append(p("As fontes externas sao instaveis por natureza. O produto comercial precisa assumir falhas como evento normal, registrar o problema e manter a aplicacao utilizavel.", styles["body"]))
    story.append(table([
        ["Fonte", "Risco", "Medida recomendada"],
        ["DataJud", "Token ausente, mudanca de payload, limite de requisicoes.", "Retries, coleta paginada, logs por classe, mocks de contrato."],
        ["DOU/IOMAT", "HTML muda, resultados incompletos.", "Parser resiliente, fallback, armazenamento de URL e texto bruto sanitizado."],
        ["SIGEF/INCRA", "WFS indisponivel ou lento.", "Fonte desabilitavel, cache, staging e upsert por codigo_imovel."],
        ["INPE", "Filtros CQL mudam, endpoints instaveis.", "Fallback por bbox, limite de registros e monitoramento de erro."],
        ["CAR", "Acesso pode exigir login ou SSL problematico.", "Tratar como fonte opcional; evitar verify=False em producao sem decisao formal."],
    ], [3.0 * cm, 6.1 * cm, 8.1 * cm], styles))
    story.append(p("Registro oficial de execucao de coleta", styles["h2"]))
    story.append(bullet_list([
        "fonte, tarefa, tenant_id quando aplicavel, horario_inicio, horario_fim.",
        "status: sucesso, parcial, falha, desabilitada.",
        "quantidade coletada, quantidade salva, duplicadas ignoradas.",
        "mensagem de erro normalizada e stack trace interno.",
        "tempo total e proxima execucao prevista.",
    ], styles))

    story.append(p("10. Alertas e Canais Comerciais", styles["h1"]))
    story.append(p("Alertas sao o centro do valor comercial. O usuario paga para ser avisado antes, com menos ruido e com contexto suficiente para agir.", styles["body"]))
    story.append(p("Tipos de alerta", styles["h2"]))
    story.append(bullet_list([
        "Nova janela quente judicial.",
        "Novo processo provavel em municipio monitorado.",
        "Nova portaria administrativa relevante.",
        "Novo evento envolvendo desapropriacao, servidao, reforma agraria ou dano ambiental.",
        "Alteracao de score de processo ja monitorado.",
        "Nova camada geoespacial relevante dentro de regiao configurada.",
    ], styles))
    story.append(p("Canais", styles["h2"]))
    story.append(bullet_list([
        "Interface web.",
        "E-mail transacional.",
        "Telegram.",
        "Webhook para clientes avancados.",
        "WhatsApp em fase posterior, com provedor aprovado.",
        "Relatorio diario ou semanal em PDF.",
    ], styles))

    story.append(p("11. Planos, Precificacao e Billing", styles["h1"]))
    story.append(table([
        ["Plano", "Publico", "Limites sugeridos"],
        ["Starter", "Perito individual em validacao inicial.", "1 usuario, 1 regiao, alertas semanais, fontes basicas."],
        ["Profissional", "Perito ativo ou pequeno escritorio.", "Ate 5 usuarios, 20 municipios, alertas diarios, exportacao CSV/PDF."],
        ["Escritorio", "Equipe juridica ou tecnica.", "Ate 20 usuarios, todo MT, webhooks, relatorios, suporte prioritario."],
        ["Enterprise", "Cliente institucional.", "Ambiente dedicado, SLA, multiestado, integracoes customizadas."],
    ], [3.0 * cm, 5.2 * cm, 9.0 * cm], styles))
    story.append(p("Billing recomendado", styles["h2"]))
    story.append(bullet_list([
        "Comecar com cobranca manual por contrato, Pix ou boleto para pilotos.",
        "Depois integrar Stripe, Mercado Pago ou gateway nacional.",
        "Registrar assinatura, vencimento, status, plano e limites.",
        "Bloquear recursos de forma gradual, sem apagar dados do cliente.",
        "Criar trilha de upgrade e downgrade.",
    ], styles))

    story.append(p("12. Observabilidade, Backups e Operacao", styles["h1"]))
    story.append(bullet_list([
        "Logs estruturados em JSON para web, worker e beat.",
        "Sentry ou equivalente para excecoes de API e workers.",
        "Metricas de latencia, erros HTTP, uso de CPU, memoria e tempo de coleta.",
        "Alertas internos se worker ou beat pararem.",
        "Backups diarios do banco e teste periodico de restauracao.",
        "Monitoramento de crescimento de tabelas geoespaciais.",
        "Painel administrativo com status das fontes.",
    ], styles))
    story.append(p("Healthchecks oficiais", styles["h2"]))
    story.append(table([
        ["Endpoint", "Uso", "Autenticacao"],
        ["/health", "Liveness simples da API.", "Nao"],
        ["/health/live", "Confirma que o processo web responde.", "Nao"],
        ["/health/ready", "Verifica banco, Redis e Celery.", "Nao, mas pode ser restrito por rede."],
        ["/api/health", "Confirma sessao de usuario autenticado.", "Sim"],
    ], [3.5 * cm, 9.0 * cm, 4.7 * cm], styles))

    story.append(p("13. Testes e Qualidade", styles["h1"]))
    story.append(p("A comercializacao exige testes automatizados para fluxos criticos. O minimo aceitavel e cobrir autenticacao, persistencia, deduplicacao, ETL e endpoints principais.", styles["body"]))
    story.append(bullet_list([
        "pytest para testes unitarios e de integracao.",
        "httpx para testar FastAPI.",
        "requests-mock ou responses para simular fontes externas.",
        "Banco de teste com PostGIS.",
        "Testes de isolamento de tenant.",
        "Smoke test pos-deploy em staging e producao.",
    ], styles))
    story.append(table([
        ["Area", "Testes obrigatorios"],
        ["Autenticacao", "Login correto, login incorreto, token ausente, token expirado, logout."],
        ["Banco", "Criacao de schema, indices, upsert de processo, deduplicacao de movimentacoes."],
        ["ETL", "CRS, geometria invalida, clip MT, remocao de dados pessoais."],
        ["Coletas", "DataJud mockado, DOU mockado, fonte indisponivel, resposta vazia."],
        ["API", "Stats, processos, eventos, GeoJSON, score, peritos e alertas."],
        ["Tenant", "Usuario de um cliente nao acessa dados privados de outro."],
    ], [3.2 * cm, 14.0 * cm], styles))

    story.append(p("14. Roadmap Cronologico", styles["h1"]))
    story.append(numbered_list([
        "Semana 1: remover segredos, corrigir healthcheck, criar .env.example, ajustar configuracao production.",
        "Semana 2: criar migrations Alembic e testes minimos de autenticacao e banco.",
        "Semana 3: subir staging com web, worker, beat, PostGIS e Redis gerenciados.",
        "Semana 4: registrar execucoes de coleta e painel de status das fontes.",
        "Semana 5: implementar tenant_id, usuarios por cliente e configuracoes por cliente.",
        "Semana 6: criar alertas configuraveis e relatorio basico.",
        "Semana 7: preparar termos, politica de privacidade, onboarding e proposta comercial.",
        "Semana 8: beta fechado com 1 a 3 usuarios reais, suporte manual e medicao de valor.",
        "Mes 3: billing, exportacoes, painel admin e melhorias de UX.",
        "Mes 4 em diante: escala, cache geoespacial, integrações e planos enterprise.",
    ], styles))

    story.append(p("15. Checklist Antes do Primeiro Cliente", styles["h1"]))
    story.append(table([
        ["Categoria", "Criterio de pronto"],
        ["Infra", "Aplicacao online com HTTPS, web/worker/beat ativos, banco e Redis estaveis."],
        ["Seguranca", "Segredos fora do Git, chaves rotacionadas, CORS restrito, login protegido."],
        ["Dados", "Coletas principais testadas, ultima coleta visivel, erros registrados."],
        ["Produto", "Dashboard, mapa, processos, eventos, alertas e score funcionando."],
        ["Comercial", "Plano, preco, contrato, onboarding e suporte definidos."],
        ["Legal", "Termos de uso, politica de privacidade e politica de fontes publicas aprovados."],
        ["Operacao", "Backup, restauracao testada, logs e monitoramento disponiveis."],
    ], [3.2 * cm, 14.0 * cm], styles))
    story.append(callout("Decisao recomendada", "Vender primeiro como beta assistido. O cliente paga por acesso e acompanhamento, enquanto o produto amadurece com dados reais, feedback real e baixo risco de escala prematura.", styles, fill="#FFF8E6", border="#C59B32"))

    story.append(p("16. Conclusao", styles["h1"]))
    story.append(p("O Radar Pericial pode evoluir para um SaaS especializado e defensavel porque combina dominio juridico, inteligencia geoespacial, dados publicos e score operacional. A prioridade nao deve ser reescrever tudo, mas profissionalizar o que ja existe: seguranca, deploy, observabilidade, multi-tenant, qualidade de dados e experiencia do usuario.", styles["body"]))
    story.append(p("A decisao tecnica mais prudente e manter Docker, separar os containers em producao, usar banco e Redis gerenciados, formalizar migrations, adicionar testes e operar inicialmente com poucos clientes de alto contato.", styles["body"]))

    return story


def build_pdf():
    OUT_DIR.mkdir(exist_ok=True)
    styles = _styles()
    doc = BaseDocTemplate(
        str(PDF_PATH),
        pagesize=A4,
        rightMargin=1.8 * cm,
        leftMargin=1.8 * cm,
        topMargin=1.7 * cm,
        bottomMargin=1.7 * cm,
        title=TITLE,
        author="Radar Pericial",
        subject=SUBTITLE,
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="normal")
    cover_frame = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id="cover")
    doc.addPageTemplates(
        [
            PageTemplate(id="cover", frames=[cover_frame], onPage=cover_page),
            PageTemplate(id="normal", frames=[frame], onPage=header_footer),
        ]
    )
    doc.build(build_story(styles))
    return PDF_PATH


if __name__ == "__main__":
    print(build_pdf())
