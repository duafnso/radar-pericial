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

# ── Configuração DB (mesma lógica do db.py) ────────────────────────────────
def get_db_url():
    url = os.getenv("DATABASE_URL")
    if not url:
        user = os.getenv("PGUSER", "postgres")
        pwd = os.getenv("PGPASSWORD", "")
        host = os.getenv("PGHOST", "localhost")
        port = os.getenv("PGPORT", "5432")
        db = os.getenv("PGDATABASE", "radar_pericial")
        url = f"postgresql://{user}:{quote_plus(pwd)}@{host}:{port}/{db}"
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url

# ── Dados Auxiliares Realistas ─────────────────────────────────────────────
IMEA_REGIOES = ["Médio-Norte", "Norte", "Centro-Sul", "Oeste", "Leste", "Sudoeste"]
MT_MUNICIPIOS = [
    "Cuiabá", "Várzea Grande", "Rondonópolis", "Sinop", "Tangará da Serra",
    "Cáceres", "Sorriso", "Lucas do Rio Verde", "Barra do Garças", "Primavera do Leste",
    "Campo Verde", "Alta Floresta", "Juína", "Peixoto de Azevedo", "Pontes e Lacerda",
    "Confresa", "Nova Mutum", "Diamantino", "Guarantã do Norte", "Matupá"
]
CLASSES_PROCESSUAIS = ["Desapropriação", "Servidão Administrativa", "Ação Possessória", "Dano Ambiental", "Usucapião Rural"]
ASSUNTOS = ["Avaliação de imóvel rural", "Indenização por benfeitorias", "Limitação administrativa", "Reintegração de posse", "Regularização fundiária"]
TIPOS_PERICIA = ["Avaliação Agronômica", "Vistoria de Benfeitorias", "Levantamento Topográfico", "Perícia Ambiental", "Análise de Uso do Solo"]
FONTES_DO = ["D.O.U.", "D.O.E.-MT", "Diário de Justiça", "Portal Gov.br", "SINFRA-MT"]

def random_date(start, end):
    delta = end - start
    int_delta = delta.days
    random_day = random.randrange(int_delta)
    return start + datetime.timedelta(days=random_day)

# ── Gerador Principal ──────────────────────────────────────────────────────
def generate_demo_data():
    logger.info("🔄 Conectando ao banco de dados...")
    engine = create_engine(get_db_url())
    
    with engine.connect() as conn:
        today = datetime.date.today()
        base_lon, base_lat = -56.0, -13.0
        
        # 1. Municípios MT (20)
        logger.info("📍 Inserindo 20 Municípios MT com geometria...")
        for i, mun in enumerate(MT_MUNICIPIOS):
            regiao = IMEA_REGIOES[i % 6]
            lon, lat = base_lon + random.uniform(-3, 3), base_lat + random.uniform(-3, 3)
            wkt = f"MULTIPOLYGON((({lon-0.5} {lat-0.5}, {lon+0.5} {lat-0.5}, {lon+0.5} {lat+0.5}, {lon-0.5} {lat+0.5}, {lon-0.5} {lat-0.5})))"
            conn.execute(text("""
                INSERT INTO municipios_mt (codigo_ibge, nome, regiao_imea, microrregiao, mesorregiao, prioridade_monitoramento, fonte, geometry)
                VALUES (:ibge, :nome, :reg, 'Microrregião Demo', 'Mesorregião Demo', 1, 'IBGE-DEMO', ST_GeomFromText(:wkt, 4326))
                ON CONFLICT (nome) DO NOTHING
            """), {"ibge": f"510{i+1:03}0", "nome": mun, "reg": regiao, "wkt": wkt})
        conn.commit()

        # 2. Parcelas SIGEF (20: 11 desapropriação, 3 vistoriados, 6 certificados)
        logger.info("🚜 Inserindo 20 Parcelas SIGEF...")
        sigef_ids = []
        for i in range(20):
            mun = random.choice(MT_MUNICIPIOS)
            area = round(random.uniform(50, 2000), 2)
            lon, lat = base_lon + random.uniform(-4, 4), base_lat + random.uniform(-4, 4)
            wkt = f"MULTIPOLYGON((({lon-0.3} {lat-0.3}, {lon+0.3} {lat-0.3}, {lon+0.3} {lat+0.3}, {lon-0.3} {lat+0.3}, {lon-0.3} {lat-0.3})))"
            
            if i < 11:
                situacao, desp_flag = "Em processo de desapropriação", True
            elif i < 14:
                situacao, desp_flag = "Vistoriado", False
            else:
                situacao, desp_flag = "Certificado", False
                
            conn.execute(text("""
                INSERT INTO parcelas_sigef (codigo_imovel, municipio, area_ha, situacao, desapropriacao_flag, tipo_camada, fonte, coletado_em, geometry)
                VALUES (:cod, :mun, :area, :sit, :desp, 'parcela_rural', 'SIGEF-DEMO', NOW(), ST_GeomFromText(:wkt, 4326))
                RETURNING id
            """), {"cod": f"SIGEF-MT-{i+1:04}", "mun": mun, "area": area, "sit": situacao, "desp": desp_flag, "wkt": wkt})
            sigef_ids.append(conn.execute(text("SELECT currval(pg_get_serial_sequence('parcelas_sigef', 'id'))")).scalar())
        conn.commit()

        # 3. Processos Judiciais (15)
        logger.info("⚖️ Inserindo 15 Processos Judiciais...")
        proc_ids = []
        for i in range(15):
            cnj = f"{random.randint(1000000, 9999999)}-{random.randint(10, 99)}.{datetime.date.today().year}.8.{random.randint(10, 99)}.{random.randint(1000, 9999)}"
            mun = random.choice(MT_MUNICIPIOS)
            regiao = IMEA_REGIOES[random.randint(0, 5)]
            conn.execute(text("""
                INSERT INTO processos (numero_cnj, tribunal, comarca, vara, classe_processual, assunto_principal, data_distribuicao, fase_atual, origem, municipio, regiao_imea, ativo, criado_em, atualizado_em)
                VALUES (:cnj, 'TJ-MT', :mun, 'Vara Agrária', :classe, :assunto, :data, 'Em andamento', 'Sistema Demo', :mun, :regiao, TRUE, NOW(), NOW())
                RETURNING id
            """), {"cnj": cnj, "mun": mun, "classe": random.choice(CLASSES_PROCESSUAIS), "assunto": random.choice(ASSUNTOS), "data": random_date(datetime.date(2023,1,1), today), "regiao": regiao})
            proc_ids.append(conn.execute(text("SELECT currval(pg_get_serial_sequence('processos', 'id'))")).scalar())
        conn.commit()

        # 4. Scores Periciais (15: 5 quente, 5 provável, 3 observação, 2 frio)
        logger.info("📊 Calculando 15 Scores Periciais...")
        faixa_map = {
            "janela_quente": (5, "🔥 Janela Quente", "Alta", "Baixa"),
            "provavel": (5, "🟠 Provável Perícia", "Média", "Média"),
            "observacao": (3, "🟡 Observação", "Baixa", "Alta"),
            "frio": (2, "❄️ Frio", "Mínima", "Nenhuma")
        }
        for i, (faixa, (count, label, tipo, urg)) in enumerate(faixa_map.items()):
            for _ in range(count):
                pid = proc_ids[i % 15]
                score = random.randint(75, 100) if faixa == "janela_quente" else \
                        random.randint(50, 74) if faixa == "provavel" else \
                        random.randint(25, 49) if faixa == "observacao" else random.randint(0, 24)
                conn.execute(text("""
                    INSERT INTO score_pericial (processo_id, score_total, score_classe, score_assunto, score_movimentacao, score_publicacao, score_administrativo, faixa_probabilidade, faixa_label, tipo_pericia_sugerida, categorias_detectadas, urgencia, calculado_em)
                    VALUES (:pid, :score, :sc, :sa, :sm, :sp, :sad, :faixa, :label, :tipo, 'Desapropriação;Avaliação Rural', :urg, NOW())
                """), {"pid": pid, "score": score, "sc": random.randint(10,30), "sa": random.randint(10,30), "sm": random.randint(10,30), "sp": random.randint(5,20), "sad": random.randint(5,20), "faixa": faixa, "label": label, "tipo": tipo, "urg": urg})
        conn.commit()

        # 5. Movimentações Processuais (9)
        logger.info("📝 Inserindo 9 Movimentações Processuais...")
        for _ in range(9):
            pid = random.choice(proc_ids)
            conn.execute(text("""
                INSERT INTO movimentacoes (processo_id, data_movimentacao, descricao, fonte, score_evento, criado_em)
                VALUES (:pid, :data, :desc, 'Sistema Demo', :score, NOW())
            """), {"pid": pid, "data": random_date(datetime.date(2024,1,1), today), "desc": random.choice(["Nomeação de perito", "Especificação de quesitos", "Apresentação de laudo preliminar", "Intimação das partes", "Audiência de conciliação"]), "score": random.randint(5, 25)})
        conn.commit()

        # 6. Portarias Diário Oficial (10)
        logger.info("📰 Inserindo 10 Portarias do Diário Oficial...")
        for i in range(10):
            mun = random.choice(MT_MUNICIPIOS)
            conn.execute(text("""
                INSERT INTO portarias_diario_oficial (titulo, resumo, data_publicacao, municipio, area_ha, fonte, orgao, url, categoria_agronomica, score_evento, faixa_probabilidade, coletado_em)
                VALUES (:titulo, :resumo, :data, :mun, :area, :fonte, :orgao, :url, 'Fundíária', :score, :faixa, NOW())
            """), {
                "titulo": f"Portaria {i+1:03}/2024 - Desapropriação Rural",
                "resumo": "Declaração de utilidade pública para reforma agrária e assentamento familiar no município.",
                "data": random_date(datetime.date(2024,1,1), today),
                "mun": mun, "area": round(random.uniform(100, 5000), 2),
                "fonte": random.choice(FONTES_DO), "orgao": "INCRA/MT",
                "url": f"https://demo.gov.br/portaria/{i+1}",
                "score": random.randint(40, 95),
                "faixa": "janela_quente" if random.random() > 0.5 else "provavel"
            })
        conn.commit()

        # 7. Eventos Administrativos (5)
        logger.info("🏛️ Inserindo 5 Eventos Administrativos...")
        for _ in range(5):
            conn.execute(text("""
                INSERT INTO eventos_administrativos (orgao, data_evento, municipio, estado, descricao, categoria, score_evento, fonte, url, area_ha, criado_em)
                VALUES (:orgao, :data, :mun, 'MT', :desc, :cat, :score, :fonte, :url, :area, NOW())
            """), {
                "orgao": random.choice(["DNIT", "SINFRA-MT", "SEMA-MT", "INCRA"]),
                "data": random_date(datetime.date(2024,1,1), today),
                "mun": random.choice(MT_MUNICIPIOS),
                "desc": "Licenciamento ambiental para implantação de corredor logístico.",
                "cat": "Infraestrutura", "score": random.randint(30, 80),
                "fonte": "Sistemas Gov", "url": "https://demo.gov.br/evt", "area": round(random.uniform(50, 1000), 2)
            })
        conn.commit()

        # 8. Assentamentos INCRA (6)
        logger.info("🌾 Inserindo 6 Assentamentos INCRA...")
        for i in range(6):
            mun = random.choice(MT_MUNICIPIOS)
            lon, lat = base_lon + random.uniform(-4, 4), base_lat + random.uniform(-4, 4)
            wkt = f"MULTIPOLYGON((({lon-0.8} {lat-0.8}, {lon+0.8} {lat-0.8}, {lon+0.8} {lat+0.8}, {lon-0.8} {lat+0.8}, {lon-0.8} {lat-0.8})))"
            conn.execute(text("""
                INSERT INTO assentamentos_incra (nome_pa, municipio, area_ha, num_familias, fase, fonte, coletado_em, geometry)
                VALUES (:nome, :mun, :area, :fam, :fase, 'INCRA-DEMO', NOW(), ST_GeomFromText(:wkt, 4326))
            """), {"nome": f"PA Esperança {i+1}", "mun": mun, "area": round(random.uniform(500, 5000), 2), "fam": random.randint(50, 300), "fase": "Consolidação", "wkt": wkt})
        conn.commit()

        # 9. Alertas DETER (5) & 10. Polígonos PRODES (5)
        logger.info("🛰️ Inserindo 5 Alertas DETER e 5 Polígonos PRODES...")
        for _ in range(5):
            lon, lat = base_lon + random.uniform(-5, 5), base_lat + random.uniform(-5, 5)
            wkt = f"MULTIPOLYGON((({lon-0.4} {lat-0.4}, {lon+0.4} {lat-0.4}, {lon+0.4} {lat+0.4}, {lon-0.4} {lat+0.4}, {lon-0.4} {lat-0.4})))"
            conn.execute(text("""
                INSERT INTO inpe_deter (view_date, classname, state, area_km2, fonte, coletado_em, geometry)
                VALUES (:data, 'DESMATAMENTO_CR', 'MT', :area, 'INPE-DEMO', NOW(), ST_GeomFromText(:wkt, 4326))
            """), {"data": random_date(datetime.date(2024,6,1), today), "area": round(random.uniform(5, 50), 2), "wkt": wkt})
            
            conn.execute(text("""
                INSERT INTO inpe_prodes (ano, estado, area_km2, classe, fonte, coletado_em, geometry)
                VALUES (2023, 'MT', :area, 'Floresta', 'INPE-PRODES-DEMO', NOW(), ST_GeomFromText(:wkt, 4326))
            """), {"area": round(random.uniform(10, 80), 2), "wkt": wkt})
        conn.commit()

        # 11. Peritos Agrônomos (6)
        logger.info("👨‍ Inserindo 6 Peritos Agrônomos...")
        nomes = ["Carlos Silva", "Ana Souza", "Roberto Lima", "Fernanda Costa", "Marcos Oliveira", "Juliana Santos"]
        for i, nome in enumerate(nomes):
            conn.execute(text("""
                INSERT INTO peritos_agronomos (nome, registro_profissional, especialidades, municipios_atuacao, regiao_imea, perfil_publico, score_profissional, criado_em)
                VALUES (:nome, :reg, :esp, :mun, :regiao, TRUE, :score, NOW())
            """), {
                "nome": nome, "reg": f"CREA-MT {random.randint(10000, 99999)}-D",
                "esp": "Desapropriação; Avaliação Rural; Georreferenciamento",
                "mun": random.choice(MT_MUNICIPIOS), "regiao": IMEA_REGIOES[i],
                "score": random.randint(60, 98)
            })
        conn.commit()

    logger.info("✅ Coleta demo finalizada com sucesso!")
    print("\n📊 Resumo dos dados inseridos:")
    print("1. Municípios MT com geometria: 20")
    print("2. Parcelas SIGEF certificadas: 20 (11 desapropriação, 3 vistoriados)")
    print("3. Processos judiciais: 15")
    print("4. Scores periciais: 15 (5 🔥 Janela Quente, 5 🟠 Provável, 3 🟡 Obs, 2 ❄️ Frio)")
    print("5. Movimentações processuais: 9")
    print("6. Portarias Diário Oficial: 10")
    print("7. Eventos administrativos: 5")
    print("8. Assentamentos INCRA: 6")
    print("9. Alertas DETER (INPE): 5")
    print("10. Polígonos PRODES: 5")
    print("11. Peritos agrônomos: 6")
    print("🌍 Regiões IMEA representadas: 6")
    print("⚠️  Terras Indígenas e Publicações DJe não possuem tabela no schema atual. Ignorados para evitar erro SQL.")

# ── Execução ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coletor de dados Radar Pericial")
    parser.add_argument("--source", type=str, default="demo", help="Fonte de dados (demo, judicial, admin, etc)")
    args = parser.parse_args()

    if args.source == "demo":
        generate_demo_data()
    else:
        logger.info(f"Fonte '{args.source}' não configurada para demo. Use --source demo")
