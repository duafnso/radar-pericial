"""
collector/judicial_collector.py
Coleta de processos judiciais: DataJud/CNJ, consulta pública TJMT, DJe
"""

import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from intelligence.taxonomy import calcular_score, municipio_para_regiao_imea

logger = logging.getLogger(__name__)

CLASSES_ALVO = [
    "desapropriação", "servidão administrativa", "ação possessória",
    "reintegração de posse", "divisão e demarcação",
    "usucapião rural", "dano ambiental",
]

KEYWORDS_DJE = [
    "desapropriação", "servidão administrativa", "perícia agronômica",
    "imóvel rural", "reforma agrária", "avaliação rural",
    "nomeação de perito",
]


def _session():
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))
    s.headers.update({"User-Agent": "RadarPericial/2.0 (dados publicos judiciais)"})
    api_key = os.getenv("DATAJUD_API_KEY", "").strip()
    if api_key:
        # DataJud usa token em header Authorization.
        s.headers.update({"Authorization": f"APIKey {api_key}"})
    else:
        logger.warning("DATAJUD_API_KEY não definida; coleta do DataJud pode falhar (401/403).")
    return s


S = _session()


def _env_enabled(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _extract_hits(payload: dict) -> list:
    hits = (payload or {}).get("hits", {})
    if isinstance(hits, dict):
        items = hits.get("hits", [])
        return items if isinstance(items, list) else []
    if isinstance(hits, list):
        return hits
    return []


# ── DataJud / CNJ ─────────────────────────────────────────────────────
def fetch_datajud(classe: str, dias_atras: int = 30, max_results: int = 100) -> list:
    url = "https://api-publica.datajud.cnj.jus.br/api_publica_tjmt/_search"
    data_ini = (datetime.now() - timedelta(days=dias_atras)).strftime("%Y-%m-%d")
    page_size = max(20, min(_env_int("DATAJUD_PAGE_SIZE", 200), 500))
    max_total = max(page_size, _env_int("DATAJUD_MAX_RESULTS_PER_CLASS", max_results))

    query_base = {
        "bool": {
            "must": [
                {"range": {"dataAjuizamento": {"gte": data_ini}}},
                {
                    "bool": {
                        "should": [
                            {"match_phrase": {"classe.nome": classe}},
                            {"match_phrase": {"classeProcessual.nome": classe}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
            ],
            "should": [
                {"match": {"assuntos.nome": kw}}
                for kw in ["rural", "imóvel", "desapropriação", "servidão", "agrícola", "agrária"]
            ],
            "minimum_should_match": 0,
        }
    }
    out = []
    try:
        offset = 0
        while len(out) < max_total:
            payload = {
                "query": query_base,
                "size": page_size,
                "from": offset,
                "sort": [{"dataAjuizamento": {"order": "desc"}}],
                "_source": [
                    "numeroProcesso", "classe", "classeProcessual", "tribunal",
                    "orgaoJulgador", "assuntos", "dataAjuizamento", "movimentos", "municipio",
                ],
            }
            r = S.post(url, json=payload, timeout=30)
            r.raise_for_status()
            hits = _extract_hits(r.json())
            if not hits:
                break
            for h in hits:
                p = _normaliza_datajud(h.get("_source", {}))
                if p:
                    out.append(p)
                if len(out) >= max_total:
                    break
            if len(hits) < page_size:
                break
            offset += page_size

        if not hits:
            # Fallback menos restritivo: só recorte temporal + palavras-chave.
            payload_fallback = {
                "query": {
                    "bool": {
                        "must": [
                            {"range": {"dataAjuizamento": {"gte": data_ini}}},
                        ],
                        "should": [
                            {"match": {"assuntos.nome": kw}}
                            for kw in ["rural", "imovel", "desapropriacao", "servidao", "agraria"]
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "size": page_size,
                "sort": [{"dataAjuizamento": {"order": "desc"}}],
                "_source": [
                    "numeroProcesso", "classe", "classeProcessual", "tribunal",
                    "orgaoJulgador", "assuntos", "dataAjuizamento", "movimentos", "municipio",
                ],
            }
            r = S.post(url, json=payload_fallback, timeout=30)
            r.raise_for_status()
            hits = _extract_hits(r.json())
            for h in hits:
                p = _normaliza_datajud(h.get("_source", {}))
                if p:
                    out.append(p)
        logger.info(f"DataJud '{classe}': {len(out)}")
    except Exception as e:
        logger.warning(f"DataJud '{classe}': {e}")
    return out


def _normaliza_datajud(src: dict) -> Optional[dict]:
    def _normalize_date(v) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        if not s:
            return ""
        # Formato ISO completo: 2025-03-19T15:22:10
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        # Formato compacto com hora: 2025031915
        if s.isdigit() and len(s) >= 8:
            return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
        # Última tentativa com parse livre
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
            except Exception:
                continue
        return ""

    def _as_text(v) -> str:
        if isinstance(v, dict):
            return str(v.get("nome") or v.get("sigla") or v.get("descricao") or "").strip()
        if isinstance(v, str):
            return v.strip()
        return ""

    def _as_nome_list(v) -> list[str]:
        if isinstance(v, list):
            out = []
            for i in v:
                t = _as_text(i)
                if t:
                    out.append(t)
            return out
        if isinstance(v, str) and v.strip():
            return [v.strip()]
        return []

    def _municipio_from_orgao(orgao_nome: str) -> str:
        if not orgao_nome:
            return ""
        m = re.search(r"Comarca de\s+([^-\n\r]+)", orgao_nome, flags=re.IGNORECASE)
        if not m:
            return ""
        return m.group(1).strip()

    numero = src.get("numeroProcesso", "")
    if not numero:
        return None
    classe = _as_text(src.get("classeProcessual")) or _as_text(src.get("classe"))
    assuntos = _as_nome_list(src.get("assuntos"))
    assunto  = assuntos[0] if assuntos else ""
    movimentos = _as_nome_list(src.get("movimentos"))
    municipio = _as_text(src.get("municipio"))
    tribunal = _as_text(src.get("tribunal")) or "TJMT"
    orgao = _as_text(src.get("orgaoJulgador"))
    if not municipio:
        municipio = _municipio_from_orgao(orgao)

    score = calcular_score(
        classe_processual=classe, assunto=assunto, movimentacoes=movimentos,
    )
    return {
        "numero_cnj":         numero,
        "tribunal":           tribunal,
        "comarca":            orgao,
        "vara":               orgao,
        "classe_processual":  classe,
        "assunto_principal":  assunto,
        "data_distribuicao":  _normalize_date(src.get("dataAjuizamento")),
        "fase_atual":         movimentos[-1] if movimentos else "",
        "origem":             "judicial",
        "municipio":          municipio,
        "regiao_imea":        municipio_para_regiao_imea(municipio),
        "_score":             score.to_dict(),
        "_movimentacoes":     movimentos,
    }


# ── DJe TJMT ─────────────────────────────────────────────────────────
def fetch_dje_tjmt(dias_atras: int = 7) -> list:
    url = "https://www.tjmt.jus.br/INTRANET.ARQ/DJMT/DiarioJustica/diario"
    out = []
    for i in range(min(dias_atras, 7)):
        data = (datetime.now() - timedelta(days=i)).strftime("%d/%m/%Y")
        try:
            r = S.get(url, params={"data": data}, timeout=30)
            if r.status_code != 200:
                continue
            soup  = BeautifulSoup(r.text, "html.parser")
            texto = " ".join(soup.find_all(string=True))
            for kw in KEYWORDS_DJE:
                if kw.lower() not in texto.lower():
                    continue
                for trecho in _trechos(texto, kw)[:3]:
                    score = calcular_score(texto_livre=trecho)
                    if score.score_total >= 25:
                        out.append({
                            "data_publicacao":   data,
                            "texto":             trecho,
                            "tipo_publicacao":   "diario_justica",
                            "palavras_detectadas": kw,
                            "orgao_origem":      "TJMT",
                            "fonte":             "DJe TJMT",
                            "score_evento":      score.score_total,
                        })
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"DJe TJMT {data}: {e}")
    logger.info(f"DJe TJMT: {len(out)} publicações")
    return out


def _trechos(texto: str, kw: str, janela: int = 200) -> list:
    out, pos = [], 0
    tl, kl = texto.lower(), kw.lower()
    while True:
        idx = tl.find(kl, pos)
        if idx == -1: break
        s = max(0, idx - janela // 2)
        e = min(len(texto), idx + janela // 2)
        out.append(texto[s:e].strip())
        pos = idx + 1
    return out


# ── Orquestrador ──────────────────────────────────────────────────────
class JudicialCollector:
    def run(self, dias_atras: int = 30) -> dict:
        processos, pubs = [], []
        if _env_enabled("ENABLE_SOURCE_DATAJUD", True):
            max_per_class = _env_int("DATAJUD_MAX_RESULTS_PER_CLASS", 1200)
            for classe in CLASSES_ALVO[:4]:
                processos.extend(fetch_datajud(classe, dias_atras=dias_atras, max_results=max_per_class))
                time.sleep(1)
            # Dedup por CNJ para reduzir repetição entre classes.
            uniq = {}
            for p in processos:
                cnj = p.get("numero_cnj")
                if cnj:
                    uniq[cnj] = p
            processos = list(uniq.values())
        else:
            logger.info("DataJud desabilitado por configuracao.")

        if _env_enabled("ENABLE_SOURCE_DJE", False):
            pubs = fetch_dje_tjmt(dias_atras=min(dias_atras, 7))
        else:
            logger.info("DJe TJMT desabilitado por configuracao.")
        logger.info(f"Judicial: {len(processos)} processos, {len(pubs)} publicações")
        return {"processos": processos, "publicacoes_dje": pubs}
