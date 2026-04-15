"""
intelligence/taxonomy.py
Taxonomia agronômica + Motor de Score Pericial — Radar Pericial v2
"""

from __future__ import annotations
from dataclasses import dataclass, field
import re

# ─────────────────────────────────────────────────────────────────────────────
# TAXONOMIA AGRONÔMICA — 13 categorias
# ─────────────────────────────────────────────────────────────────────────────
TAXONOMIA: dict[str, dict] = {
    "desapropriacao": {
        "label": "Desapropriação",
        "peso": 30,
        "keywords": [
            "desapropriação", "desapropriacao", "utilidade pública", "utilidade publica",
            "interesse social", "decreto expropriatório", "edital de desapropriação",
            "imissão na posse", "justa indenização", "reforma agrária", "reforma agraria",
            "decreto declaratório", "expropriação",
        ],
    },
    "servidao_administrativa": {
        "label": "Servidão Administrativa",
        "peso": 28,
        "keywords": [
            "servidão administrativa", "servidao administrativa", "faixa de domínio",
            "faixa de dominio", "faixa de servidão", "passagem de dutos",
            "linha de transmissão", "servidão de passagem", "area non aedificandi",
        ],
    },
    "avaliacao_rural": {
        "label": "Avaliação de Imóvel Rural",
        "peso": 25,
        "keywords": [
            "avaliação de imóvel rural", "avaliacao rural", "valor de mercado",
            "valor venal", "laudo de avaliação", "avaliação de terra nua",
            "benfeitorias", "gleba", "fazenda", "sítio", "chácara", "imóvel rural",
        ],
    },
    "conflito_fundiario": {
        "label": "Conflito Fundiário",
        "peso": 22,
        "keywords": [
            "conflito fundiário", "ação possessória", "reintegração de posse",
            "manutenção de posse", "interdito proibitório", "turbação", "esbulho",
            "usucapião rural", "regularização fundiária", "titulação", "ocupação irregular",
        ],
    },
    "dano_agricola": {
        "label": "Dano Agrícola",
        "peso": 20,
        "keywords": [
            "dano em lavoura", "dano agrícola", "perda de safra", "quebra de safra",
            "frustração de safra", "dano em plantação", "produção agrícola",
            "soja", "milho", "algodão", "cana-de-açúcar", "carne bovina",
        ],
    },
    "produtividade": {
        "label": "Produtividade Agrícola",
        "peso": 18,
        "keywords": [
            "produtividade", "produção por hectare", "capacidade produtiva",
            "exploração econômica", "aptidão agrícola", "potencial produtivo",
            "zoneamento agrícola",
        ],
    },
    "benfeitorias": {
        "label": "Benfeitorias Rurais",
        "peso": 20,
        "keywords": [
            "benfeitoria necessária", "benfeitoria útil", "benfeitoria voluptuária",
            "infraestrutura rural", "pivô de irrigação", "silo", "armazém",
            "curral", "cercas", "casa sede", "casa de funcionários",
        ],
    },
    "georreferenciamento": {
        "label": "Georreferenciamento",
        "peso": 18,
        "keywords": [
            "georreferenciamento", "confrontação", "memorial descritivo",
            "planta planimétrica", "levantamento topográfico", "certificação de imóvel",
            "retificação de área", "sobreposição de áreas", "SIGEF", "SNCR",
        ],
    },
    "uso_solo": {
        "label": "Uso e Ocupação do Solo",
        "peso": 16,
        "keywords": [
            "uso do solo", "ocupação do solo", "zoneamento", "aptidão pedológica",
            "classificação de solo", "solo agricultável", "plano diretor rural",
        ],
    },
    "dano_ambiental_rural": {
        "label": "Dano Ambiental Rural",
        "peso": 20,
        "keywords": [
            "dano ambiental", "área de preservação permanente", "APP",
            "reserva legal", "supressão vegetal", "desmatamento",
            "degradação ambiental", "passivo ambiental", "recuperação de área degradada",
            "licenciamento ambiental", "RIMA", "EIA",
        ],
    },
    "regularizacao_fundiaria": {
        "label": "Regularização Fundiária",
        "peso": 18,
        "keywords": [
            "regularização fundiária", "regularizacao fundiaria", "titulação",
            "assentamento", "INCRA", "terra devoluta", "aforamento",
            "concessão de uso", "imóvel público",
        ],
    },
    "infraestrutura_rural": {
        "label": "Infraestrutura Rural",
        "peso": 22,
        "keywords": [
            "obra de infraestrutura", "rodovia", "ferrovia", "hidrovia",
            "DNIT", "SINFRA", "duplicação de rodovia", "ampliação de rodovia",
            "obra viária", "faixa de drenagem",
        ],
    },
    "inventario_rural": {
        "label": "Inventário com Imóvel Rural",
        "peso": 14,
        "keywords": [
            "inventário", "partilha de imóvel rural", "herança rural",
            "divisão de terra", "espólio", "herdeiros",
        ],
    },
}

# Palavras que indicam perícia JÁ em andamento (sinal forte)
KEYWORDS_PERICIAIS = [
    "perícia", "perito", "quesitos", "assistente técnico",
    "laudo pericial", "honorários periciais", "nomeação de perito",
    "fixação de honorários", "apresentação de laudo",
    "complementação de laudo", "esclarecimentos do perito",
    "laudo agronômico", "vistoria",
]

# Regiões IMEA do Mato Grosso
REGIOES_IMEA: dict[str, list[str]] = {
    "Norte": [
        "Alta Floresta", "Guarantã do Norte", "Peixoto de Azevedo",
        "Colíder", "Nova Bandeirantes", "Apiacás", "Carlinda",
    ],
    "Médio-Norte": [
        "Sinop", "Sorriso", "Lucas do Rio Verde", "Nova Mutum",
        "Vera", "Santa Rita do Trivelato", "Diamantino", "Tapurah",
    ],
    "Leste": [
        "Barra do Garças", "Água Boa", "Nova Xavantina",
        "Canarana", "Querência", "São Félix do Araguaia",
        "Confresa", "Porto Alegre do Norte",
    ],
    "Centro-Sul": [
        "Cuiabá", "Várzea Grande", "Rondonópolis", "Primavera do Leste",
        "Jaciara", "Juscimeira", "Campo Verde", "Santo Antônio do Leverger",
    ],
    "Oeste": [
        "Cáceres", "Pontes e Lacerda", "Mirassol d'Oeste",
        "Tangará da Serra", "Barra do Bugres", "Salto do Céu",
    ],
    "Sudoeste": [
        "Juína", "Juara", "Brasnorte", "Sapezal", "Campos de Júlio",
        "Comodoro", "Conquista d'Oeste",
    ],
}

# Score por classe processual
SCORE_CLASSE: dict[str, int] = {
    "desapropriação": 30, "servidão administrativa": 28,
    "ação possessória": 22, "reintegração de posse": 20,
    "divisão e demarcação": 18, "usucapião": 18,
    "dano ambiental": 20, "ação indenizatória": 15,
    "inventário": 12, "default": 5,
}

# Score por assunto
SCORE_ASSUNTO: dict[str, int] = {
    "avaliação de imóvel rural": 25, "benfeitorias": 22,
    "produtividade agrícola": 20, "danos em lavoura": 20,
    "georreferenciamento": 18, "uso do solo": 16,
    "dano ambiental rural": 20, "reforma agrária": 18,
    "servidão": 22, "confrontação": 18, "default": 5,
}

# Score por movimentação processual
SCORE_MOVIMENTACAO: dict[str, int] = {
    "especificação de provas": 20, "apresentação de quesitos": 25,
    "nomeação de perito": 30, "fixação de honorários": 28,
    "manifestação de assistente técnico": 22, "despacho saneador": 15,
    "apresentação de laudo": 30, "complementação de laudo": 20,
    "esclarecimentos do perito": 18, "default": 2,
}

# Score por evento administrativo
SCORE_EVENTO_ADMIN: dict[str, int] = {
    "decreto de utilidade pública": 30, "portaria de desapropriação": 30,
    "edital de desapropriação": 28, "faixa de servidão": 25,
    "projeto de duplicação": 22, "obra viária": 20,
    "regularização fundiária": 18, "licença ambiental": 12,
    "default": 5,
}

# Faixas de probabilidade
FAIXAS = [
    (0,  24,  "frio",           "❄️ Frio"),
    (25, 49,  "observacao",     "👁️ Observação"),
    (50, 74,  "provavel",       "⚠️ Provável perícia"),
    (75, 100, "janela_quente",  "🔥 Janela quente"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Dataclass de resultado
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class ScorePericial:
    score_total: int = 0
    score_classe: int = 0
    score_assunto: int = 0
    score_movimentacao: int = 0
    score_publicacao: int = 0
    score_administrativo: int = 0
    faixa: str = "frio"
    faixa_label: str = "❄️ Frio"
    tipo_pericia_sugerida: str = ""
    categorias_detectadas: list = field(default_factory=list)
    urgencia: str = "baixa"

    def to_dict(self) -> dict:
        return {
            "score_total":           self.score_total,
            "score_classe":          self.score_classe,
            "score_assunto":         self.score_assunto,
            "score_movimentacao":    self.score_movimentacao,
            "score_publicacao":      self.score_publicacao,
            "score_administrativo":  self.score_administrativo,
            "faixa_probabilidade":   self.faixa,
            "faixa_label":           self.faixa_label,
            "tipo_pericia_sugerida": self.tipo_pericia_sugerida,
            "categorias_detectadas": ",".join(self.categorias_detectadas),
            "urgencia":              self.urgencia,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Funções internas
# ─────────────────────────────────────────────────────────────────────────────
def _faixa(score: int) -> tuple[str, str]:
    for low, high, key, label in FAIXAS:
        if low <= score <= high:
            return key, label
    return "frio", "❄️ Frio"


def _match(valor: str, score_dict: dict) -> int:
    v = valor.lower() if valor else ""
    for chave, pts in score_dict.items():
        if chave != "default" and chave in v:
            return pts
    return score_dict.get("default", 3)


def _analisa_texto(texto: str) -> tuple[int, list[str]]:
    if not texto:
        return 0, []
    tl = texto.lower()
    score, cats = 0, []
    for kw in KEYWORDS_PERICIAIS:
        if kw.lower() in tl:
            score += 8
    for cat_key, cat in TAXONOMIA.items():
        hits = sum(1 for kw in cat["keywords"] if kw.lower() in tl)
        if hits:
            cats.append(cat_key)
            score += min(cat["peso"] * hits, cat["peso"] * 2)
    return min(score, 40), cats


# ─────────────────────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────────────────────
def municipio_para_regiao_imea(municipio: str) -> str:
    if not municipio:
        return "Centro-Sul"
    for regiao, municipios in REGIOES_IMEA.items():
        if any(municipio.lower() in m.lower() for m in municipios):
            return regiao
    return "Centro-Sul"


def calcular_score(
    classe_processual: str = "",
    assunto: str = "",
    movimentacoes: list = None,
    publicacoes: list = None,
    eventos_admin: list = None,
    texto_livre: str = "",
) -> ScorePericial:
    r = ScorePericial()
    r.score_classe       = _match(classe_processual, SCORE_CLASSE)
    r.score_assunto      = _match(assunto, SCORE_ASSUNTO)
    r.score_movimentacao = max((_match(m, SCORE_MOVIMENTACAO) for m in (movimentacoes or [])), default=0)
    r.score_administrativo = max((_match(e, SCORE_EVENTO_ADMIN) for e in (eventos_admin or [])), default=0)

    textos = list(publicacoes or []) + ([texto_livre] if texto_livre else [])
    pub_score, cats = 0, []
    for t in textos:
        s, c = _analisa_texto(t)
        pub_score = max(pub_score, s)
        cats.extend(c)
    r.score_publicacao = pub_score
    r.categorias_detectadas = list(dict.fromkeys(cats))  # dedup mantendo ordem

    total = (
        r.score_classe          * 0.20 +
        r.score_assunto         * 0.20 +
        r.score_movimentacao    * 0.25 +
        r.score_publicacao      * 0.20 +
        r.score_administrativo  * 0.15
    )
    r.score_total = min(int(total * 2.5), 100)
    r.faixa, r.faixa_label = _faixa(r.score_total)

    if r.categorias_detectadas:
        r.tipo_pericia_sugerida = TAXONOMIA.get(r.categorias_detectadas[0], {}).get("label", "")

    r.urgencia = "alta" if r.score_total >= 75 else "media" if r.score_total >= 50 else "baixa"
    return r


def classificar_texto(texto: str) -> dict:
    s = calcular_score(texto_livre=texto)
    return {
        "score":                s.score_total,
        "faixa":                s.faixa,
        "faixa_label":          s.faixa_label,
        "categorias":           s.categorias_detectadas,
        "tipo_pericia_sugerida": s.tipo_pericia_sugerida,
        "urgencia":             s.urgencia,
    }
