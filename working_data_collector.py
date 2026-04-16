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
    
    # Corrige URL para SQLAlchemy 2.x
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url

# ── Dados Auxiliares ─────────────────────────────────────────────────────
IMEA_REGIOES = ["Médio-Norte", "Norte", "Centro-Sul", "Oeste", "Leste", "Sudoeste"]
MT_MUNICIPIOS = [
    "Cuiabá", "Várzea Grande", "Rondonópolis", "Sinop", "Tangará da Serra",
    "Cáceres", "Sorriso", "Lucas do Rio Verde", "Barra do Garças", "Primavera do Leste",
    "Campo Verde", "Alta Floresta", "Juína", "Peixoto de Azevedo", "Pontes e Lacerda",
    "Confresa", "Nova Mutum", "Diamantino", "Guarantã do Norte", "Matupá"
]
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
        logger.error(f"❌ Falha ao conectar: {e}")
        sys.exit(1)

    with engine.connect() as conn:
        today = datetime.date.today()
        base_lon, base_lat = -56.0, -13.0
        
        # 1. Municípios (20) - ✅ CORRIGIDO: usa codigo_ibge para ON CONFLICT
        logger.info("📍 Inserindo 20 Municípios MT...")
        for i, mun in enumerate(MT_MUNICIPIOS):
            regiao = IMEA_REGIOES[i % 6]
            lon, lat = base_lon + random.uniform(-3, 3), base_lat + random.uniform(-3, 3)
            wkt = f"MULTIPOLYGON((({lon-0.5} {lat-0.5}, {lon+0.5} {lat-0.5}, {lon+0.5} {lat+0.5}, {lon-0.5} {lat+0.5}, {lon-0.5} {lat-0.5})))"
            conn.execute(text("""
                INSERT INTO municipios_mt (codigo_ibge, nome, regiao_imea, microrregiao, mesorregiao, prioridade_monitoramento, fonte, geometry)
                VALUES (:ibge, :nome, :reg, 'Microrregião Demo', 'Mesorregião Demo', 1, 'IBGE-DEMO', ST_GeomFromText(:wkt, 4326))
                ON CONFLICT (codigo_ibge) DO NOTHING
            """), {"ibge": f"510{i+1:03}0", "nome": mun, "reg": regiao, "wkt": wkt})
        conn.commit()

        # 2. Parcelas SIGEF (20)
        logger.info("🚜 Inserindo 20 Parcelas SIGEF...")
        for i in range(20):
            mun = random.choice(MT_MUNICIPIOS)
            area = round(random.uniform(50, 2000), 2)
            lon, lat = base_lon + random.uniform(-4, 4), base_lat + random.uniform(-4, 4)
            wkt = f"MULTIPOLYGON((({lon-0.3} {lat-0.3}, {lon+0.3} {lat-0.3}, {lon+0.3} {lat+0.3}, {lon-0.3} {lat+0.3}, {lon-0.3} {lat-0.3})))"
            if i < 11: sit, desp = "Em processo de desapropriação", True
            elif i < 14: sit, desp = "Vistoriado", False
            else: sit, desp = "Certificado", False
            conn.execute(text("""
                INSERT INTO parcelas_sigef (codigo_imovel, municipio, area_ha, situacao, desapropriacao_flag, tipo_camada, fonte, coletado_em, geometry)
                VALUES (:cod, :mun, :area, :sit, :desp, 'parcela_rural', 'SIGEF-DEMO', NOW(), ST_GeomFromText(:wkt, 4326))
                ON CONFLICT (codigo_imovel) DO NOTHING
            """), {"cod": f"SIGEF-MT-{i+1:04}", "mun": mun, "area": area, "sit": sit, "desp": desp, "wkt": wkt})
        conn.commit()

        # 3. Processos (15)
        logger.info("⚖️ Inserindo 15 Processos Judiciais...")
        proc_ids = []
        for i in range(15):
            cnj = f"{random.randint(1000000, 9999999)}-{random.randint(10, 99)}.{today.year}.8.{random.randint(10, 99)}.{random.randint(1000, 9999)}"
            mun = random.choice(MT_MUNICIPIOS)
            regiao = IMEA_REGIOES[random.randint(0, 5)]
            result = conn.execute(text("""
                INSERT INTO processos (numero_cnj, tribunal, comarca, vara, classe_processual, assunto_principal, data_distribuicao, fase_atual, origem, municipio, regiao_imea, ativo, criado_em, atualizado_em)
                VALUES (:cnj, 'TJ-MT', :mun, 'Vara Agrária', :classe, :assunto, :data, 'Em andamento', 'Sistema Demo', :mun, :regiao, TRUE, NOW(), NOW())
                ON CONFLICT (numero_cnj) DO NOTHING
                RETURNING id
            """), {"cnj": cnj, "mun": mun, "classe": random.choice(CLASSES_PROCESSUAIS), "assunto": random.choice(ASSUNTOS), "data": random_date(datetime.date(2023,1,1), today), "regiao": regiao})
            row = result.fetchone()
            if row:
                proc_ids.append(row[0])
            else:
                # Se já existia, busca o ID
                existing = conn.execute(text("SELECT id FROM processos WHERE numero_cnj = :cnj"), {"cnj": cnj}).fetchone()
                if existing:
                    proc_ids.append(existing[0])
        conn.commit()

        # 4. Scores (15)
        logger.info("📊 Calculando 15 Scores Periciais...")
        faixa_map = {"janela_quente": (5, "🔥 Janela Quente", "Alta"), "provavel": (5, "🟠 Provável Perícia", "Média"), "observacao": (3, "🟡 Observação", "Baixa"), "frio": (2, "❄️ Frio", "Mínima")}
        idx = 0
        for faixa, (count, label, urg) in faixa_map.items():
            for _ in range(count):
                if idx < len(proc_ids):
                    pid = proc_ids[idx]
                    score = random.randint(75, 100) if faixa=="janela_quente" else random.randint(50, 74) if faixa=="provavel" else random.randint(25, 49) if faixa=="observacao" else random.randint(0, 24)
                    conn.execute(text("""
                        INSERT INTO score_pericial (processo_id, score_total, score_classe, score_assunto, score_movimentacao, score_publicacao, score_administrativo, faixa_probabilidade, faixa_label, tipo_pericia_sugerida, categorias_detectadas, urgencia, calculado_em)
                        VALUES (:pid, :score, 15, 15, 15, 10, 10, :faixa, :label, 'Avaliação Agronômica', 'Desapropriação;Avaliação Rural', :urg, NOW())
                        ON CONFLICT (processo_id) DO NOTHING
                    """), {"pid": pid, "score": score, "faixa": faixa, "label": label, "urg": urg})
                    idx += 1
        conn.commit()

        # 5. Movimentações (9)
        logger.info("📝 Inserindo 9 Movimentações...")
        for _ in range(9):
            if proc_ids:
                conn.execute(text("""
                    INSERT INTO movimentacoes (processo_id, data_movimentacao, descricao, fonte, score_evento, criado_em)
                    VALUES (:pid, :data, :desc, 'Sistema Demo', 15, NOW())
                """), {"pid": random.choice(proc_ids), "data": random_date(datetime.date(2024,1,1), today), "desc": random.choice(["Nomeação de perito", "Especificação de quesitos", "Apresentação de laudo", "Intimação das partes"])})
        conn.commit()

        # 6. Portarias DO (10)
        logger.info("📰 Inserindo 10 Portarias D.O....")
        for i in range(10):
            conn.execute(text("""
                INSERT INTO portarias_diario_oficial (titulo, resumo, data_publicacao, municipio, area_ha, fonte, orgao, url, categoria_agronomica, score_evento, faixa_probabilidade, coletado_em)
                VALUES (:titulo, 'Declaração de utilidade pública para reforma agrária.', :data, :mun, :area, :fonte, 'INCRA/MT', 'https://demo.gov.br', 'Fundíária', :score, :faixa, NOW())
            """), {"titulo": f"Portaria {i+1:03}/2024", "data": random_date(datetime.date(2024,1,1), today), "mun": random.choice(MT_MUNICIPIOS), "area": round(random.uniform(100, 5000), 2), "fonte": random.choice(FONTES_DO), "score": random.randint(40, 95), "faixa": "janela_quente" if random.random()>0.5 else "provavel"})
        conn.commit()

        # 7. Eventos Admin (5)
        logger.info("🏛️ Inserindo 5 Eventos Administrativos...")
        for _ in range(5):
            conn.execute(text("""
                INSERT INTO eventos_administrativos (orgao, data_evento, municipio, estado, descricao, categoria, score_evento, fonte, url, area_ha, criado_em)
                VALUES ('DNIT', :data, :mun, 'MT', 'Licenciamento ambiental para corredor logístico.', 'Infraestrutura', 50, 'Sistemas Gov', 'https://demo.gov.br', 500, NOW())
            """), {"data": random_date(datetime.date(2024,1,1), today), "mun": random.choice(MT_MUNICIPIOS)})
        conn.commit()

        # 8. Assentamentos (6)
        logger.info("🌾 Inserindo 6 Assentamentos INCRA...")
        for i in range(6):
            lon, lat = base_lon + random.uniform(-4, 4), base_lat + random.uniform(-4, 4)
            wkt = f"MULTIPOLYGON((({lon-0.8} {lat-0.8}, {lon+0.8} {lat-0.8}, {lon+0.8} {lat+0.8}, {lon-0.8} {lat+0.8}, {lon-0.8} {lat-0.8})))"
            conn.execute(text("""
                INSERT INTO assentamentos_incra (nome_pa, municipio, area_ha, num_familias, fase, fonte, coletado_em, geometry)
                VALUES (:nome, :mun, :area, :fam, 'Consolidação', 'INCRA-DEMO', NOW(), ST_GeomFromText(:wkt, 4326))
            """), {"nome": f"PA Esperança {i+1}", "mun": random.choice(MT_MUNICIPIOS), "area": round(random.uniform(500, 5000), 2), "fam": random.randint(50, 300), "wkt": wkt})
        conn.commit()

        # 9. DETER (5) & 10. PRODES (5)
        logger.info("🛰️ Inserindo 5 DETER e 5 PRODES...")
        for _ in range(5):
            lon, lat = base_lon + random.uniform(-5, 5), base_lat + random.uniform(-5, 5)
            wkt = f"MULTIPOLYGON((({lon-0.4} {lat-0.4}, {lon+0.4} {lat-0.4}, {lon+0.4} {lat+0.4}, {lon-0.4} {lat+0.4}, {lon-0.4} {lat-0.4})))"
            conn.execute(text("""
                INSERT INTO inpe_deter (view_date, classname, state, area_km2, fonte, coletado_em, geometry)
                VALUES (:data, 'DESMATAMENTO_CR', 'MT', :area, 'INPE-DEMO', NOW(), ST_GeomFromText(:wkt, 4326))
            """), {"data": random_date(datetime.date(2024,6,1), today), "area": round(random.uniform(5, 50), 2), "wkt": wkt})
            conn.execute(text("""
                INSERT INTO inpe_prodes (ano, estado, area_km2, classe, fonte, coletado_em, geometry)
                VALUES (2023, 'MT', :area, 'Floresta', 'INPE-PRODES', NOW(), ST_GeomFromText(:wkt, 4326))
            """), {"area": round(random.uniform(10, 80), 2), "wkt": wkt})
        conn.commit()

        # 11. Peritos (6)
        logger.info("👨‍ Inserindo 6 Peritos Agrônomos...")
        for i, nome in enumerate(["Carlos Silva", "Ana Souza", "Roberto Lima", "Fernanda Costa", "Marcos Oliveira", "Juliana Santos"]):
            conn.execute(text("""
                INSERT INTO peritos_agronomos (nome, registro_profissional, especialidades, municipios_atuacao, regiao_imea, perfil_publico, score_profissional, criado_em)
                VALUES (:nome, :reg, 'Desapropriação;Avaliação Rural', :mun, :regiao, TRUE, :score, NOW())
            """), {"nome": nome, "reg": f"CREA-MT {random.randint(10000,99999)}-D", "mun": random.choice(MT_MUNICIPIOS), "regiao": IMEA_REGIOES[i], "score": random.randint(60,98)})
        conn.commit()

    logger.info("✅ Coleta demo finalizada com sucesso!")
    print("\n📊 Dados inseridos conforme solicitado:")
    print("✅ 20 Municípios | 20 Parcelas SIGEF | 15 Processos | 15 Scores")
    print("✅ 9 Movimentações | 10 Portarias DO | 5 Eventos Admin")
    print("✅ 6 Assentamentos | 5 DETER | 5 PRODES | 6 Peritos")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=str, default="demo")
    args = parser.parse_args()
    if args.source == "demo":
        generate_demo_data()
