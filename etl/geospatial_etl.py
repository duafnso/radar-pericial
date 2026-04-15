"""
etl/geospatial_etl.py
Pipeline ETL geoespacial: CRS, geometrias inválidas, clip MT, dedup, LGPD
"""

import logging
from typing import Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from shapely.validation import make_valid

logger = logging.getLogger(__name__)

BBOX_MT = box(-61.6, -18.1, -50.2, -7.3)

COLUNAS_PESSOAIS = ["cpf","cnpj","nome_prop","proprietar","email","telefone","fone","celular","rg","documento"]

STATUS_DESAPROPRIACAO = [
    "em processo de desapropriacao","desapropriado",
    "declarado de interesse social","vistoriado",
    "em fase de obtencao","interesse social",
]


def clean_gdf(gdf: gpd.GeoDataFrame, name: str = "") -> gpd.GeoDataFrame:
    if gdf is None or gdf.empty:
        return gdf
    n0 = len(gdf)

    # CRS
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Geometrias inválidas
    inv = ~gdf.geometry.is_valid
    if inv.any():
        gdf.loc[inv, "geometry"] = gdf.loc[inv, "geometry"].apply(make_valid)

    # Remove nulas/vazias
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]

    # Clip ao MT
    gdf = gdf[gdf.geometry.intersects(BBOX_MT)]

    # Dedup por hash da geometria
    try:
        gdf["_h"] = gdf.geometry.apply(lambda g: hash(g.wkb))
        gdf = gdf.drop_duplicates(subset="_h").drop(columns=["_h"])
    except Exception:
        pass

    # Remove dados pessoais (LGPD)
    drop = [c for c in gdf.columns if any(k in c.lower() for k in COLUNAS_PESSOAIS)]
    if drop:
        gdf = gdf.drop(columns=drop)

    if len(gdf) != n0:
        logger.info(f"ETL {name}: {n0} → {len(gdf)}")
    return gdf.reset_index(drop=True)


def enrich_municipio(gdf: gpd.GeoDataFrame, municipios: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf is None or gdf.empty or municipios is None or municipios.empty:
        return gdf
    try:
        centroids = gdf.copy()
        # Evita centroid em CRS geográfico (warning e precisão ruim).
        proj = gdf.to_crs(epsg=5880)
        centroids["geometry"] = proj.geometry.centroid.to_crs(gdf.crs)
        nome_col = next((c for c in ["nome","NM_MUN","NM_MUNICIP"] if c in municipios.columns), None)
        mun_sel = municipios[["geometry"] + ([nome_col] if nome_col else [])].copy()
        if nome_col:
            mun_sel = mun_sel.rename(columns={nome_col: "_mun_nome"})
        joined = gpd.sjoin(centroids, mun_sel, how="left", predicate="within")
        if nome_col and "municipio" not in gdf.columns and "_mun_nome" in joined.columns:
            joined["municipio"] = joined["_mun_nome"]
        if "index_right" in joined.columns:
            joined = joined.drop(columns=["index_right"])
        if "_mun_nome" in joined.columns:
            joined = joined.drop(columns=["_mun_nome"])
        joined["geometry"] = gdf.geometry.values
        return gpd.GeoDataFrame(joined, geometry="geometry", crs=gdf.crs)
    except Exception as e:
        logger.warning(f"enrich_municipio: {e}")
        return gdf


def flag_desapropriacao(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "situacao" in gdf.columns:
        gdf["desapropriacao_flag"] = gdf["situacao"].str.lower().apply(
            lambda s: any(k in str(s) for k in STATUS_DESAPROPRIACAO)
        )
    return gdf


def run_etl(raw: dict, municipios: gpd.GeoDataFrame = None) -> dict:
    cleaned = {}
    for name, gdf in raw.items():
        if gdf is None:
            cleaned[name] = None
            continue
        if not isinstance(gdf, gpd.GeoDataFrame):
            cleaned[name] = gdf
            continue
        gdf = clean_gdf(gdf, name=name)
        gdf = flag_desapropriacao(gdf)
        if municipios is not None and "municipio" not in gdf.columns:
            gdf = enrich_municipio(gdf, municipios)
        cleaned[name] = gdf
    return cleaned
