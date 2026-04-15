"""
collector/admin_collector.py
Eventos administrativos: DOU, IOMAT-MT, DNIT, SINFRA-MT
Robusto contra respostas não-JSON e redirecionamentos.
"""

import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup

from intelligence.taxonomy import classificar_texto

logger = logging.getLogger(__name__)

RE_AREA  = re.compile(r"(\d[\d.,]+)\s*(?:ha|hectares?)", re.I)
RE_MUNIC = re.compile(
    r"(?:município|municipio|localizado em|situado em|no município de)\s+(?:de\s+)?"
    r"([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Úa-zà-ú]+)*)", re.I
)
RE_DATE = re.compile(r"\d{2}/\d{2}/\d{4}|\d{4}-\d{2}-\d{2}")

ORGAOS = {
    "INCRA": "INCRA", "DNIT": "DNIT", "SINFRA": "SINFRA-MT",
    "IBAMA": "IBAMA", "SEMA": "SEMA-MT",
    "ANTT": "ANTT",
}


def _session() -> requests.Session:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    s.mount("https://", HTTPAdapter(
        max_retries=Retry(total=2, backoff_factor=1,
                          status_forcelist=[429, 500, 502, 503, 504])
    ))
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.9",
        "Accept-Language": "pt-BR,pt;q=0.9",
    })
    return s


S = _session()


def _env_enabled(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _safe_json(r: requests.Response) -> Optional[dict]:
    """Retorna dict apenas se a resposta for JSON válido."""
    ct = r.headers.get("Content-Type", "")
    if "json" not in ct and "javascript" not in ct:
        return None
    try:
        return r.json()
    except Exception:
        return None


def _meta(texto: str) -> dict:
    am = RE_AREA.search(texto)
    mm = RE_MUNIC.search(texto)
    dm = RE_DATE.search(texto)
    return {
        "area_ha":   float(am.group(1).replace(".", "").replace(",", ".")) if am else None,
        "municipio": mm.group(1).strip() if mm else None,
        "data_str":  dm.group(0) if dm else None,
    }


def _orgao(texto: str) -> str:
    tu = texto.upper()
    for sigla, nome in ORGAOS.items():
        if sigla in tu:
            return nome
    return "Órgão Público"


def _dedup(items: list, key: str = "titulo") -> list:
    seen, out = set(), []
    for i in items:
        k = str(i.get(key, "")).strip()[:120]
        if not k:
            k = str(i.get("url", "")).strip() or str(i.get("resumo", "")).strip()[:120]
        if k and k not in seen:
            seen.add(k)
            out.append(i)
    return out


def _make_item(titulo, resumo, fonte, orgao, url=None,
               data_str=None, municipio=None, area_ha=None) -> dict:
    titulo = (titulo or "").strip() or f"{fonte} — Evento administrativo"
    resumo = (resumo or "").strip() or "Sem resumo disponível."
    score = classificar_texto(f"{titulo} {resumo}")
    return {
        "titulo":               titulo[:400],
        "resumo":               resumo[:600],
        "data_publicacao":      data_str,
        "municipio":            municipio,
        "area_ha":              area_ha,
        "fonte":                fonte,
        "orgao":                orgao,
        "url":                  url,
        "categoria_agronomica": (score.get("categorias") or [""])[0],
        "score_evento":         score.get("score", 0),
        "faixa_probabilidade":  score.get("faixa", "frio"),
        "coletado_em":          datetime.utcnow().isoformat(),
    }


# ── DOU — portal in.gov.br ────────────────────────────────────────────────
def fetch_dou(dias_atras: int = 30) -> list:
    di = (datetime.now() - timedelta(days=dias_atras)).strftime("%d/%m/%Y")
    df = datetime.now().strftime("%d/%m/%Y")
    out = []

    keywords = [
        "desapropriação INCRA Mato Grosso",
        "servidão administrativa Mato Grosso",
        "imóvel rural MT INCRA decreto",
        "reforma agrária Mato Grosso",
    ]

    for kw in keywords:
        try:
            r = S.get(
                "https://www.in.gov.br/consulta",
                params={
                    "q": kw,
                    "exactDate": "personalizado",
                    "inicialDate": di,
                    "finalDate": df,
                    "s": "todos",
                    "delta": 20,
                },
                timeout=30,
            )
            if r.status_code != 200:
                logger.warning(f"DOU '{kw}': HTTP {r.status_code}")
                time.sleep(1.0)
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select(
                ".resultado-item, article.resultado, "
                ".search-result, .resultado, li.resultado"
            )

            for item in items:
                tit = item.select_one("h5, h4, h3, .titulo-do-ato, .title")
                cor = item.select_one(".corpo-do-ato, .description, p")
                if not tit:
                    continue
                titulo = tit.get_text(" ", strip=True)
                corpo  = cor.get_text(" ", strip=True) if cor else ""
                texto  = f"{titulo} {corpo}"
                if not re.search(r"\bM\.?T\.?\b|Mato\s+Grosso", texto, re.I):
                    continue
                meta = _meta(texto)
                a = item.select_one("a[href]")
                url = None
                if a:
                    h = a.get("href", "")
                    url = h if h.startswith("http") else "https://www.in.gov.br" + h
                out.append(_make_item(
                    titulo=titulo, resumo=corpo[:500],
                    fonte="DOU", orgao=_orgao(titulo),
                    url=url, data_str=meta.get("data_str"),
                    municipio=meta.get("municipio"),
                    area_ha=meta.get("area_ha"),
                ))
            time.sleep(1.2)

        except Exception as e:
            logger.warning(f"DOU '{kw}': {e}")

    # Segunda tentativa via API JSON do in.gov.br
    if not out:
        try:
            r = S.get(
                "https://www.in.gov.br/en/web/dou/-/desapropriacao-incra-mato-grosso",
                timeout=20,
            )
            # Só loga se conseguiu — não é crítico
        except Exception:
            pass

    logger.info(f"DOU: {len(out)} resultados")
    return out


# ── IOMAT — Diário Oficial MT ─────────────────────────────────────────────
def fetch_iomat(dias_atras: int = 30) -> list:
    """
    O IOMAT publica PDFs e HTML por edição.
    Tenta o buscador web e, se falhar, lê as últimas edições por data.
    """
    out = []

    # Opção 1: buscador do portal
    endpoints_busca = [
        "https://www.iomat.mt.gov.br/portal/pesquisa",
        "https://www.iomat.mt.gov.br/pesquisa",
        "https://iomat.mt.gov.br/busca",
    ]
    keywords = ["desapropriação", "INCRA", "servidão administrativa", "imóvel rural"]

    for ep in endpoints_busca:
        for kw in keywords:
            try:
                r = S.get(ep, params={"q": kw}, timeout=20)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                items = soup.select(".resultado, .item-busca, article, li.ato")
                for item in items:
                    tit = item.select_one("h3, h4, strong, b, a")
                    if not tit:
                        continue
                    texto = item.get_text(" ", strip=True)
                    meta = _meta(texto)
                    a = item.select_one("a[href]")
                    href = a.get("href", "") if a else ""
                    url = href if href.startswith("http") else (
                        "https://www.iomat.mt.gov.br" + href if href else None
                    )
                    out.append(_make_item(
                        titulo=tit.get_text(strip=True)[:300],
                        resumo=texto[:500],
                        fonte="IOMAT-MT", orgao="IOMAT",
                        url=url, data_str=meta.get("data_str"),
                        municipio=meta.get("municipio"),
                        area_ha=meta.get("area_ha"),
                    ))
                time.sleep(0.8)
            except Exception:
                continue
        if out:
            break

    # Opção 2: lê as últimas N edições por data
    if not out:
        for dias_offset in range(0, min(dias_atras, 10)):
            data = (datetime.now() - timedelta(days=dias_offset)).strftime("%Y/%m/%d")
            try:
                r = S.get(
                    f"https://www.iomat.mt.gov.br/portal/visualizacoes/html/{data}/",
                    timeout=15,
                )
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                texto_pagina = soup.get_text(" ", strip=True)
                for kw in ["desapropriação", "INCRA", "servidão", "imóvel rural"]:
                    if kw.lower() in texto_pagina.lower():
                        meta = _meta(texto_pagina)
                        out.append(_make_item(
                            titulo=f"IOMAT {data} — contém '{kw}'",
                            resumo=texto_pagina[:500],
                            fonte="IOMAT-MT", orgao="IOMAT",
                            url=f"https://www.iomat.mt.gov.br/portal/visualizacoes/html/{data}/",
                            data_str=data.replace("/", "-"),
                            municipio=meta.get("municipio"),
                            area_ha=meta.get("area_ha"),
                        ))
                        break
            except Exception:
                continue

    logger.info(f"IOMAT: {len(out)} resultados")
    return out


# ── DNIT ──────────────────────────────────────────────────────────────────
def fetch_dnit() -> list:
    out = []

    # PNCP — Portal Nacional de Contratações Públicas (API REST oficial)
    try:
        r = S.get(
            "https://pncp.gov.br/api/pncp/v1/contratos",
            params={
                "ufSigla": "MT",
                "dataInicial": (datetime.now() - timedelta(days=90)).strftime("%Y%m%d"),
                "dataFinal": datetime.now().strftime("%Y%m%d"),
                "pagina": 1,
                "tamanhoPagina": 20,
            },
            timeout=30,
        )
        data = _safe_json(r)
        if data and r.status_code == 200:
            for item in data.get("data", []):
                obj = str(item.get("objetoContrato", "") or "")
                if not re.search(
                    r"rodovia|pavimento|obra|estrada|faixa|terreno|rural|duplica",
                    obj, re.I
                ):
                    continue
                meta = _meta(obj)
                out.append(_make_item(
                    titulo=f"PNCP/DNIT — {obj[:200]}",
                    resumo=obj[:400],
                    fonte="DNIT", orgao="DNIT",
                    data_str=(item.get("dataAssinatura") or "")[:10],
                    municipio=meta.get("municipio"),
                    area_ha=meta.get("area_ha"),
                ))
        logger.info(f"DNIT PNCP: {len(out)} contratos MT")
    except Exception as e:
        logger.warning(f"DNIT PNCP: {e}")

    # Fallback scraping gov.br/dnit
    if not out:
        for url_try in [
            "https://www.gov.br/dnit/pt-br/assuntos/noticias",
            "https://www.gov.br/dnit/pt-br/noticias-e-eventos/noticias",
        ]:
            try:
                r = S.get(url_try, timeout=25)
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                for item in soup.select("article, .tileItem, .listingItem"):
                    texto = item.get_text(" ", strip=True)
                    if "MT" not in texto and "Mato Grosso" not in texto:
                        continue
                    h = item.select_one("h2, h3, a")
                    meta = _meta(texto)
                    a = item.select_one("a[href]")
                    href = a.get("href", "") if a else ""
                    url = href if href.startswith("http") else (
                        "https://www.gov.br" + href if href else None
                    )
                    out.append(_make_item(
                        titulo=h.get_text(strip=True)[:200] if h else texto[:100],
                        resumo=texto[:400],
                        fonte="DNIT", orgao="DNIT",
                        url=url, data_str=meta.get("data_str"),
                        municipio=meta.get("municipio"),
                        area_ha=meta.get("area_ha"),
                    ))
                if out:
                    break
            except Exception as e:
                logger.warning(f"DNIT {url_try}: {e}")

    logger.info(f"DNIT total: {len(out)}")
    return out


# ── SINFRA-MT ─────────────────────────────────────────────────────────────
def fetch_sinfra() -> list:
    out = []
    urls_try = [
        "https://www.mt.gov.br/noticias-e-eventos",
        "https://www.mt.gov.br/secretarias-estado/31",
        "https://www.sinfra.mt.gov.br/noticias",
        "https://www.sinfra.mt.gov.br/",
    ]
    for url in urls_try:
        try:
            r = S.get(url, timeout=20)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for item in soup.select("article, .noticia, .item, .news-item, li.item"):
                texto = item.get_text(" ", strip=True)
                if len(texto) < 40:
                    continue
                score = classificar_texto(texto)
                if score.get("score", 0) < 8:
                    continue
                meta = _meta(texto)
                h = item.select_one("h2, h3, h4, a")
                a = item.select_one("a[href]")
                href = a.get("href", "") if a else ""
                item_url = href if href.startswith("http") else (
                    "https://www.sinfra.mt.gov.br" + href if href else None
                )
                out.append(_make_item(
                    titulo=h.get_text(strip=True)[:200] if h else texto[:100],
                    resumo=texto[:400],
                    fonte="SINFRA-MT", orgao="SINFRA-MT",
                    url=item_url, data_str=meta.get("data_str"),
                    municipio=meta.get("municipio"),
                    area_ha=meta.get("area_ha"),
                ))
            if out:
                break
        except Exception as e:
            logger.warning(f"SINFRA-MT {url}: {e}")

    logger.info(f"SINFRA-MT: {len(out)}")
    return out


# ── Orquestrador ──────────────────────────────────────────────────────────
class AdminCollector:
    def run(self, dias_atras: int = 30) -> list:
        out = []
        if _env_enabled("ENABLE_SOURCE_DOU", True):
            out.extend(fetch_dou(dias_atras=dias_atras))
        else:
            logger.info("DOU desabilitado por configuracao.")
        if _env_enabled("ENABLE_SOURCE_IOMAT", False):
            out.extend(fetch_iomat(dias_atras=dias_atras))
        else:
            logger.info("IOMAT desabilitado por configuracao.")
        if _env_enabled("ENABLE_SOURCE_DNIT", True):
            out.extend(fetch_dnit())
        else:
            logger.info("DNIT desabilitado por configuracao.")
        if _env_enabled("ENABLE_SOURCE_SINFRA", False):
            out.extend(fetch_sinfra())
        else:
            logger.info("SINFRA desabilitado por configuracao.")
        result = _dedup(out, key="titulo")
        logger.info(f"Admin total: {len(result)} eventos")
        return result
