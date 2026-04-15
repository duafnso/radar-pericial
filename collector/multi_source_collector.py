"""
collector/multi_source_collector.py
Coleta geoespacial: IBGE (municípios MT), INCRA/SIGEF, INPE, CAR
URLs verificadas março/2026
"""

import io
import json
import logging
import os
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import box, shape

logger = logging.getLogger(__name__)

BBOX_MT = (-61.6, -18.1, -50.2, -7.3)
MT_IBGE_CODE = 51
TMP = Path("/tmp/radar_cache")
TMP.mkdir(exist_ok=True)

STATUS_DESAPROPRIACAO = [
    "em processo de desapropriacao", "desapropriado",
    "declarado de interesse social", "vistoriado",
    "em fase de obtencao", "interesse social",
]


def _session() -> requests.Session:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    s = requests.Session()
    s.mount("https://", HTTPAdapter(
        max_retries=Retry(total=3, backoff_factor=1,
                          status_forcelist=[500, 502, 503, 504])
    ))
    s.headers.update({
        "User-Agent": "RadarPericial/2.0 (dados publicos BR)",
        "Accept": "application/json,application/geo+json,*/*",
    })
    return s


S = _session()


def _env_enabled(name: str, default: bool = True) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


# ── IBGE — Municípios MT ──────────────────────────────────────────────────
def fetch_ibge_municipios() -> Optional[gpd.GeoDataFrame]:
    """
    Usa a API de malhas municipais do IBGE para baixar os 141 polígonos de MT.
    Endpoint correto: /api/v3/malhas/estados/{cod}/municipios
    """
    # Endpoint principal e variantes (a API do IBGE muda parâmetros com frequência).
    attempts = [
        (
            f"https://servicodados.ibge.gov.br/api/v3/malhas/estados/{MT_IBGE_CODE}/municipios",
            {"formato": "application/vnd.geo+json"},
        ),
        (
            f"https://servicodados.ibge.gov.br/api/v3/malhas/estados/{MT_IBGE_CODE}/municipios",
            {"formato": "application/json"},
        ),
        (
            f"https://servicodados.ibge.gov.br/api/v2/malhas/{MT_IBGE_CODE}",
            {"resolucao": 5, "formato": "application/vnd.geo+json"},
        ),
    ]
    try:
        gdf = None
        last_err = None
        for url, params in attempts:
            try:
                r = S.get(url, params=params, timeout=120)
                r.raise_for_status()
                ct = (r.headers.get("Content-Type") or "").lower()
                if "json" in ct:
                    try:
                        payload = r.json()
                        if isinstance(payload, dict) and payload.get("features"):
                            gdf = gpd.GeoDataFrame.from_features(payload["features"], crs="EPSG:4326")
                        else:
                            gdf = gpd.read_file(io.BytesIO(r.content))
                    except Exception:
                        gdf = gpd.read_file(io.BytesIO(r.content))
                else:
                    gdf = gpd.read_file(io.BytesIO(r.content))
                if gdf is not None and not gdf.empty:
                    break
            except Exception as err:
                last_err = err
                continue

        if gdf is None or gdf.empty:
            raise ValueError(f"GDF vazio para municípios MT ({last_err})")

        # Busca os nomes dos municípios via API de localidades
        info = _fetch_ibge_info()
        if info is not None and "codarea" in gdf.columns:
            gdf = gdf.merge(info, left_on="codarea", right_on="codigo_ibge", how="left")
        elif info is not None:
            # Tenta pelo índice se não houver codarea
            if len(gdf) == len(info):
                for col in ["nome", "microrregiao", "mesorregiao", "codigo_ibge"]:
                    if col in info.columns:
                        gdf[col] = info[col].values

        if "nome" not in gdf.columns:
            for cand in ["NM_MUN", "nome_municipio", "name"]:
                if cand in gdf.columns:
                    gdf = gdf.rename(columns={cand: "nome"})
                    break
        if "nome" not in gdf.columns:
            gdf["nome"] = [f"Municipio MT {i+1}" for i in range(len(gdf))]

        # Adiciona região IMEA estimada por bbox
        gdf["regiao_imea"] = gdf["nome"].apply(_regiao_imea_por_nome)
        gdf["fonte"] = "IBGE"
        gdf["tipo_camada"] = "limite_municipal"

        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        elif not gdf.crs:
            gdf = gdf.set_crs(epsg=4326)

        logger.info(f"IBGE: {len(gdf)} municípios MT")
        return gdf

    except Exception as e:
        logger.error(f"IBGE municípios: {e}")
        return _fetch_ibge_municipios_fallback()


def _fetch_ibge_municipios_fallback() -> Optional[gpd.GeoDataFrame]:
    """
    Fallback: baixa GeoJSON de municípios MT do GitHub (dados IBGE espelhados).
    """
    urls_fallback = [
        f"https://servicodados.ibge.gov.br/api/v2/malhas/{MT_IBGE_CODE}?resolucao=5&formato=application%2Fvnd.geo%2Bjson",
        f"https://servicodados.ibge.gov.br/api/v2/malhas/{MT_IBGE_CODE}?formato=application%2Fjson",
    ]
    for url in urls_fallback:
        try:
            r = S.get(url, timeout=60)
            r.raise_for_status()
            ct = (r.headers.get("Content-Type") or "").lower()
            if "json" in ct:
                try:
                    payload = r.json()
                    if isinstance(payload, dict) and payload.get("features"):
                        gdf = gpd.GeoDataFrame.from_features(payload["features"], crs="EPSG:4326")
                    else:
                        gdf = gpd.read_file(io.BytesIO(r.content))
                except Exception:
                    gdf = gpd.read_file(io.BytesIO(r.content))
            else:
                gdf = gpd.read_file(io.BytesIO(r.content))
            if not gdf.empty:
                if "nome" not in gdf.columns:
                    gdf["nome"] = [f"Municipio MT {i+1}" for i in range(len(gdf))]
                gdf["fonte"] = "IBGE"
                gdf["tipo_camada"] = "limite_municipal"
                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)
                elif not gdf.crs:
                    gdf = gdf.set_crs(epsg=4326)
                logger.info(f"IBGE fallback: {len(gdf)} registros")
                return gdf
        except Exception as e:
            logger.warning(f"IBGE fallback {url}: {e}")
    return None


def _fetch_ibge_info() -> Optional[pd.DataFrame]:
    """Baixa metadados dos municípios MT (nomes, microrregiões)."""
    url = f"https://servicodados.ibge.gov.br/api/v1/localidades/estados/{MT_IBGE_CODE}/municipios"
    try:
        r = S.get(url, timeout=30)
        r.raise_for_status()
        rows = []
        for m in r.json():
            mr = m.get("microrregiao") or {}
            meso = (mr.get("mesorregiao") or {}) if isinstance(mr, dict) else {}
            rows.append({
                "codigo_ibge": str(m.get("id", "")),
                "nome": m.get("nome", ""),
                "microrregiao": mr.get("nome", "") if isinstance(mr, dict) else "",
                "mesorregiao": meso.get("nome", "") if isinstance(meso, dict) else "",
            })
        return pd.DataFrame(rows)
    except Exception as e:
        logger.error(f"IBGE info: {e}")
        return None


def fetch_ibge_estado() -> Optional[gpd.GeoDataFrame]:
    """Baixa o polígono do estado MT (usado como limite_estado_mt)."""
    url = f"https://servicodados.ibge.gov.br/api/v3/malhas/estados/{MT_IBGE_CODE}"
    try:
        r = S.get(url, params={
            "formato": "application/vnd.geo+json",
            "resolucao": 2,
        }, timeout=30)
        r.raise_for_status()
        gdf = gpd.read_file(io.BytesIO(r.content))
        gdf["nome"] = "Mato Grosso"
        gdf["fonte"] = "IBGE"
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(epsg=4326)
        elif not gdf.crs:
            gdf = gdf.set_crs(epsg=4326)
        return gdf
    except Exception as e:
        logger.error(f"IBGE estado: {e}")
        return None


def fetch_ibge_info() -> Optional[pd.DataFrame]:
    return _fetch_ibge_info()


REGIOES_IMEA_COORDS = {
    "Norte":      (-58.5, -10.5),
    "Médio-Norte": (-56.0, -12.0),
    "Leste":      (-52.0, -13.5),
    "Centro-Sul": (-55.5, -15.5),
    "Oeste":      (-59.5, -15.0),
    "Sudoeste":   (-58.0, -17.0),
}

MUNICIPIOS_REGIAO = {
    "Sinop": "Médio-Norte", "Sorriso": "Médio-Norte",
    "Lucas do Rio Verde": "Médio-Norte", "Nova Mutum": "Médio-Norte",
    "Alta Floresta": "Norte", "Juara": "Norte", "Colider": "Norte",
    "Guarantã do Norte": "Norte", "Peixoto de Azevedo": "Norte",
    "Cuiabá": "Centro-Sul", "Várzea Grande": "Centro-Sul",
    "Rondonópolis": "Centro-Sul", "Primavera do Leste": "Leste",
    "Barra do Garças": "Leste", "Água Boa": "Leste",
    "Cáceres": "Oeste", "Pontes e Lacerda": "Oeste",
    "Tangará da Serra": "Oeste", "Juína": "Oeste",
    "Jaciara": "Sudoeste", "São Félix do Araguaia": "Leste",
}


def _regiao_imea_por_nome(nome: str) -> str:
    if not nome:
        return "Centro-Sul"
    return MUNICIPIOS_REGIAO.get(str(nome).strip(), "Centro-Sul")


# ── INCRA / SIGEF ─────────────────────────────────────────────────────────
def fetch_sigef_parcelas() -> Optional[gpd.GeoDataFrame]:
    """
    Tenta WFS do INCRA/SIGEF. Se falhar, tenta CSV shapefile.
    """
    wfs_endpoints = [
        {
            "url": "https://geoserver.incra.gov.br/geoserver/wfs",
            "layer": "sigef:parcela_certificada",
        },
        {
            "url": "https://geoserver2.incra.gov.br/geoserver/wfs",
            "layer": "sigef:parcela_certificada",
        },
    ]

    for ep in wfs_endpoints:
        try:
            r = S.get(ep["url"], params={
                "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                "typeName": ep["layer"], "outputFormat": "application/json",
                "count": 5000,
                "bbox": f"{BBOX_MT[0]},{BBOX_MT[1]},{BBOX_MT[2]},{BBOX_MT[3]},EPSG:4326",
                "srsName": "EPSG:4326",
            }, timeout=120)
            r.raise_for_status()
            gdf = gpd.read_file(io.BytesIO(r.content))
            if not gdf.empty:
                logger.info(f"SIGEF WFS: {len(gdf)} parcelas")
                return _normaliza_sigef(gdf)
        except Exception as e:
            logger.warning(f"SIGEF WFS {ep['url']}: {e}")

    # Fallback CSV/SHP do SIGEF
    return _sigef_csv_fallback()


def _sigef_csv_fallback() -> Optional[gpd.GeoDataFrame]:
    """Baixa shapefile do SIGEF via certificacao.incra.gov.br"""
    urls = [
        "https://certificacao.incra.gov.br/csv_shp/export_shp.py?tipo_func_query=SHP_SIGEF_PUBLICO&uf=MT",
        "https://acervofundiario.incra.gov.br/i3geo/ogc.php?service=WFS&version=2.0.0&request=GetFeature&typeName=sigef_publico_mt&outputFormat=application/json",
    ]
    for url in urls:
        try:
            r = S.get(url, timeout=180, stream=True, allow_redirects=True)
            r.raise_for_status()
            content = b"".join(r.iter_content(8192))
            if not content:
                continue

            # Tenta como ZIP (shapefile)
            try:
                zp = TMP / "sigef_mt.zip"
                zp.write_bytes(content)
                with zipfile.ZipFile(zp) as z:
                    z.extractall(TMP / "sigef_mt")
                shp = list((TMP / "sigef_mt").glob("*.shp"))
                if shp:
                    gdf = gpd.read_file(shp[0])
                    logger.info(f"SIGEF SHP: {len(gdf)} parcelas")
                    return _normaliza_sigef(gdf)
            except zipfile.BadZipFile:
                pass

            # Tenta como GeoJSON direto
            try:
                as_text = content[:2000].decode("utf-8", errors="ignore").lower()
                if "<html" in as_text or "<!doctype" in as_text:
                    raise ValueError("Resposta HTML (nao geoespacial)")
                gdf = gpd.read_file(io.BytesIO(content))
                if not gdf.empty:
                    logger.info(f"SIGEF GeoJSON: {len(gdf)} parcelas")
                    return _normaliza_sigef(gdf)
            except Exception as ge:
                logger.warning(f"SIGEF fallback parse: {ge}")

        except Exception as e:
            logger.warning(f"SIGEF fallback {url}: {e}")

    logger.warning("SIGEF: todas as fontes falharam")
    return None


def _normaliza_sigef(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    col_map = {
        "cod_imovel": "codigo_imovel", "parcela_co": "codigo_imovel",
        "municipio_": "municipio",     "nom_munici": "municipio",
        "area_total": "area_ha",       "num_area": "area_ha",
        "sit_imovel": "situacao",
    }
    for old, new in col_map.items():
        if old in gdf.columns and new not in gdf.columns:
            gdf = gdf.rename(columns={old: new})

    # Remove dados pessoais (LGPD)
    drop = [c for c in gdf.columns if any(
        k in c.lower() for k in ["cpf", "cnpj", "nome_prop", "proprietar", "detentor"]
    )]
    if drop:
        gdf = gdf.drop(columns=drop)

    if "situacao" in gdf.columns:
        gdf["desapropriacao_flag"] = gdf["situacao"].str.lower().fillna("").apply(
            lambda s: any(k in s for k in STATUS_DESAPROPRIACAO)
        )
    else:
        gdf["desapropriacao_flag"] = False

    gdf["fonte"] = "INCRA/SIGEF"
    gdf["tipo_camada"] = "parcela_rural"
    gdf["coletado_em"] = datetime.utcnow().isoformat()

    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    elif not gdf.crs:
        gdf = gdf.set_crs(epsg=4326)

    logger.info(f"SIGEF normalizado: {len(gdf)} parcelas, "
                f"{gdf['desapropriacao_flag'].sum()} em desapropriação")
    return gdf


# ── INCRA Assentamentos ───────────────────────────────────────────────────
def fetch_assentamentos() -> Optional[gpd.GeoDataFrame]:
    """
    Tenta a API REST do INCRA e o endpoint do dados.gov.br como fallback.
    Sempre retorna GeoDataFrame (com geometrias nulas se a API não fornecer
    coordenadas), garantindo compatibilidade com o pipeline ETL geoespacial.
    """
    urls = [
        "https://www.incra.gov.br/api/assentamento/?uf=MT&format=json&limit=500",
        "https://dados.gov.br/api/publico/conjuntos-dados/incra-assentamentos/recursos?limit=5",
    ]
    for url in urls:
        try:
            r = S.get(url, timeout=30, allow_redirects=False)
            if r.status_code in (301, 302, 307, 308):
                logger.warning(f"Assentamentos: redirect para {r.headers.get('Location','?')}")
                continue
            r.raise_for_status()
            data = r.json()
            items = data.get("results", data) if isinstance(data, dict) else data
            if items and len(items) > 0:
                df = pd.DataFrame(items)
                df["fonte"] = "INCRA/Assentamentos"
                df["tipo_camada"] = "assentamento"

                # Fix: converte para GeoDataFrame.
                # Tenta lat/lon se disponíveis; caso contrário cria geometrias nulas
                # para manter compatibilidade com o ETL (evita crash no clean_gdf).
                from shapely.geometry import Point
                lat_col = next(
                    (c for c in df.columns if c.lower() in ("lat", "latitude")), None
                )
                lon_col = next(
                    (c for c in df.columns if c.lower() in ("lon", "lng", "longitude")), None
                )
                if lat_col and lon_col:
                    def _ponto(row):
                        try:
                            return Point(float(row[lon_col]), float(row[lat_col]))
                        except (TypeError, ValueError):
                            return None
                    df["geometry"] = df.apply(_ponto, axis=1)
                else:
                    df["geometry"] = None

                gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
                logger.info(f"Assentamentos: {len(gdf)} registros")
                return gdf
        except Exception as e:
            logger.warning(f"Assentamentos {url}: {e}")
    return None


# ── INPE PRODES ───────────────────────────────────────────────────────────
def fetch_inpe_prodes() -> Optional[gpd.GeoDataFrame]:
    """
    Tenta múltiplos endpoints do TerraBrasilis/INPE para PRODES.
    URLs atualizadas março/2026.
    """
    endpoints = [
        {
            "url": "https://terrabrasilis.dpi.inpe.br/geoserver/prodes-cerrado-nb/ows",
            "layer": "prodes-cerrado-nb:yearly_deforestation",
            "filter": "uf='MT' AND year>=2022",
        },
        {
            "url": "https://terrabrasilis.dpi.inpe.br/geoserver/prodes-amz-nb/ows",
            "layer": "prodes-amz-nb:yearly_deforestation_biome",
            "filter": "state='MT' AND year>=2022",
        },
        {
            "url": "https://terrabrasilis.dpi.inpe.br/geoserver/prodes-legal-amz-nb/ows",
            "layer": "prodes-legal-amz-nb:yearly_deforestation_biome",
            "filter": "state='Mato Grosso' AND year>=2022",
        },
    ]

    for ep in endpoints:
        try:
            r = S.get(ep["url"], params={
                "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                "typeName": ep["layer"],
                "outputFormat": "application/json",
                "count": 1000,
                "CQL_FILTER": ep["filter"],
                "srsName": "EPSG:4326",
            }, timeout=60)

            if r.status_code == 400:
                # Tenta sem filtro CQL e com bbox
                r = S.get(ep["url"], params={
                    "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                    "typeName": ep["layer"],
                    "outputFormat": "application/json",
                    "count": 500,
                    "bbox": f"{BBOX_MT[0]},{BBOX_MT[1]},{BBOX_MT[2]},{BBOX_MT[3]},EPSG:4326",
                    "srsName": "EPSG:4326",
                }, timeout=60)

            r.raise_for_status()
            gdf = gpd.read_file(io.BytesIO(r.content))
            if not gdf.empty:
                gdf["fonte"] = "INPE/PRODES"
                gdf["tipo_camada"] = "desmatamento"
                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)
                elif not gdf.crs:
                    gdf = gdf.set_crs(epsg=4326)
                logger.info(f"INPE PRODES: {len(gdf)} polígonos")
                return gdf
        except Exception as e:
            logger.warning(f"INPE PRODES {ep['url']}: {e}")

    return None


# ── INPE DETER ────────────────────────────────────────────────────────────
def fetch_inpe_deter() -> Optional[gpd.GeoDataFrame]:
    endpoints = [
        {
            "url": "https://terrabrasilis.dpi.inpe.br/geoserver/deter-amz/ows",
            "layer": "deter-amz:deter_public",
            "filter": "state='Mato Grosso'",
        },
        {
            "url": "https://terrabrasilis.dpi.inpe.br/geoserver/deter-cerrado/ows",
            "layer": "deter-cerrado:deter_public",
            "filter": "uf='MT'",
        },
    ]
    for ep in endpoints:
        try:
            r = S.get(ep["url"], params={
                "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                "typeName": ep["layer"],
                "outputFormat": "application/json",
                "count": 500,
                "CQL_FILTER": ep["filter"],
                "srsName": "EPSG:4326",
            }, timeout=60)
            if r.status_code == 400:
                r = S.get(ep["url"], params={
                    "service": "WFS", "version": "2.0.0", "request": "GetFeature",
                    "typeName": ep["layer"],
                    "outputFormat": "application/json",
                    "count": 500,
                    "bbox": f"{BBOX_MT[0]},{BBOX_MT[1]},{BBOX_MT[2]},{BBOX_MT[3]},EPSG:4326",
                    "srsName": "EPSG:4326",
                }, timeout=60)
            r.raise_for_status()
            gdf = gpd.read_file(io.BytesIO(r.content))
            if not gdf.empty:
                gdf["fonte"] = "INPE/DETER"
                gdf["tipo_camada"] = "alerta_desmatamento"
                if gdf.crs and gdf.crs.to_epsg() != 4326:
                    gdf = gdf.to_crs(epsg=4326)
                elif not gdf.crs:
                    gdf = gdf.set_crs(epsg=4326)
                logger.info(f"INPE DETER: {len(gdf)} alertas")
                return gdf
        except Exception as e:
            logger.warning(f"INPE DETER {ep['url']}: {e}")
    return None


# ── CAR ───────────────────────────────────────────────────────────────────
def fetch_car() -> Optional[gpd.GeoDataFrame]:
    """
    CAR/SICAR — tenta download do shapefile por estado.
    Se o site exigir login, retorna None silenciosamente.
    """
    try:
        r = S.get(
            "https://www.car.gov.br/publico/estados/downloads",
            params={"sigla": "MT"},
            timeout=60, stream=True, allow_redirects=True,
            verify=False,  # SSL problemático no servidor deles
        )
        r.raise_for_status()
        content = b"".join(r.iter_content(8192))
        if len(content) < 1000:
            raise ValueError("Resposta muito pequena — provável redirect para login")

        zp = TMP / "car_mt.zip"
        zp.write_bytes(content)
        ed = TMP / "car_mt"
        ed.mkdir(exist_ok=True)
        with zipfile.ZipFile(zp) as z:
            z.extractall(ed)
        shp = list(ed.glob("**/*.shp"))
        if shp:
            gdf = gpd.read_file(shp[0])
            if gdf.crs and gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs(epsg=4326)
            drop = [c for c in gdf.columns if any(
                k in c.lower() for k in ["cpf", "cnpj", "nome_prop"]
            )]
            if drop:
                gdf = gdf.drop(columns=drop)
            gdf["fonte"] = "CAR/SICAR"
            gdf["tipo_camada"] = "cadastro_ambiental"
            logger.info(f"CAR: {len(gdf)} imóveis")
            return gdf
    except Exception as e:
        logger.warning(f"CAR: {e}")
    return None


# ── Orquestrador ──────────────────────────────────────────────────────────
class MultiSourceCollector:
    SOURCES = {
        "municipios_mt":    fetch_ibge_municipios,
        "limite_estado":    fetch_ibge_estado,
        "info_municipios":  fetch_ibge_info,
        "sigef_parcelas":   fetch_sigef_parcelas,
        "assentamentos":    fetch_assentamentos,
        "desmatamento":     fetch_inpe_prodes,
        "alertas_deter":    fetch_inpe_deter,
        "car":              fetch_car,
    }

    def run(self, sources: list = None) -> dict:
        sel = {k: v for k, v in self.SOURCES.items()
               if sources is None or k in sources}
        enabled_map = {
            "municipios_mt": _env_enabled("ENABLE_SOURCE_IBGE_MUNICIPIOS", True),
            "limite_estado": _env_enabled("ENABLE_SOURCE_IBGE_ESTADO", True),
            "info_municipios": _env_enabled("ENABLE_SOURCE_IBGE_INFO", True),
            "sigef_parcelas": _env_enabled("ENABLE_SOURCE_SIGEF", False),
            "assentamentos": _env_enabled("ENABLE_SOURCE_ASSENTAMENTOS", False),
            "desmatamento": _env_enabled("ENABLE_SOURCE_PRODES", True),
            "alertas_deter": _env_enabled("ENABLE_SOURCE_DETER", False),
            "car": _env_enabled("ENABLE_SOURCE_CAR", False),
        }
        results = {}
        for name, fn in sel.items():
            if not enabled_map.get(name, True):
                logger.info(f"Coletando: {name} (desabilitada por configuracao)")
                results[name] = None
                continue
            logger.info(f"Coletando: {name}")
            t0 = time.time()
            try:
                results[name] = fn()
            except Exception as e:
                logger.error(f"{name}: {e}")
                results[name] = None
            n = len(results[name]) if results[name] is not None else 0
            logger.info(f"  {name}: {n} registros ({time.time()-t0:.1f}s)")
        return results
