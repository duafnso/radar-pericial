#!/usr/bin/env python3
"""
working_data_collector.py — Gerador de dados DEMO para Radar Pericial
Popula o banco com dados fictícios realistas de Mato Grosso.
Uso: python working_data_collector.py --source demo
"""

import os
import sys
import random
import datetime
import argparse
import logging
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def get_db_url():
    url = os.getenv("DATABASE_URL")
    if not url:
        user = os.getenv("PGUSER", "postgres")
        pwd = os.getenv("PGPASSWORD", "")
        host = os.getenv("PGHOST", "localhost")
        port = os.getenv("PGPORT", "5432")
        db = os.getenv("PGDATABASE", "radar_pericial")
        url = f"postgresql://{user}:{quote_plus(pwd)}@{host}:{port}/{db}"
    
    # ✅ Correção SQLAlchemy 2.x: força dialect correto
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url

IMEA_REGIOES = ["Médio-Norte", "Norte", "Centro-Sul", "Oeste", "Leste", "Sudoeste"]
MT_MUNICIPIOS = ["Cuiabá", "Várzea Grande", "Rondonópolis", "Sinop", "Tangará da Serra",
                 "Cáceres", "Sorriso", "Lucas do Rio Verde", "Barra do Garças", "Primavera do Leste",
                 "Campo Verde", "Alta Floresta", "Juína", "Peixoto de Azevedo", "Pontes e Lacerda",
                 "Confresa", "Nova Mutum", "Diamantino", "Guarantã do Norte", "Matupá"]
CLASSES_PROCESSUAIS = ["Desapropriação", "Servidão Administrativa", "Ação Possessória", "Dano Ambiental", "Usucapião Rural"]
ASSUNTOS = ["Avaliação de imóvel rural", "Indenização por benfeitorias", "Limitação administrativa", "Reintegração de posse", "Regularização fundiária"]
FONTES_DO = ["D.O.U.", "D.O.E.-MT", "Diário de Justiça", "Portal Gov.br", "SINFRA-MT"]

def random_date(start, end):
    delta = end - start
    return start + datetime.timedelta(days=random.randrange(delta.days))

def generate_demo_data():
    logger.info("🔄 Conectando ao banco de dados...")
    try:
        engine = create_engine(get_db_url(), pool_pre_ping=True)
    except Exception as e:
        logger.error(f"❌ Fal
