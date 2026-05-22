#!/usr/bin/env python3
"""
consulta_desapropiacao_jaciara.py

Consulta as desapropriações de terras ao redor da cidade de Jaciara, MT
no período de 2026, e gera um relatório em texto.

Uso:
  python consulta_desapropiacao_jaciara.py
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────
def main():
    """
    Executa a consulta de desapropriações em Jaciara, MT para 2026.
    """
    try:
        from database.db import Database
        from collector.multi_source_collector import MultiSourceCollector
        from etl.geospatial_etl import run_etl
        import geopandas as gpd
        import pandas as pd
        
        logger.info("=== Iniciando consulta de desapropriações em Jaciara, MT ===")
        
        # Inicializa banco de dados
        db = Database()
        logger.info("✓ Conectado ao banco de dados")
        
        # Coleta dados geoespaciais
        logger.info("→ Coletando dados geoespaciais (IBGE, SIGEF, INCRA)...")
        raw = MultiSourceCollector().run()
        logger.info(f"✓ Dados coletados: {len(raw)} camadas")
        
        # Executa ETL para limpeza e normalização
        logger.info("→ Processando ETL...")
        cleaned = run_etl(raw, municipios=raw.get("municipios_mt"))
        logger.info("✓ ETL concluído")
        
        # Extrai parcelas SIGEF com desapropriação
        sigef = cleaned.get("sigef_parcelas")
        municipios_mt = cleaned.get("municipios_mt")
        
        resultados = []
        
        if sigef is not None and hasattr(sigef, "empty") and not sigef.empty:
            logger.info(f"→ Consultando {len(sigef)} parcelas SIGEF...")
            
            # Filtra apenas desapropriações ativas
            ativas = sigef[sigef["desapropriacao_flag"] == True]
            logger.info(f"  - {len(ativas)} parcelas em desapropriação")
            
            # Filtra para Jaciara
            jaciara_desaprop = ativas[
                ativas["municipio"].str.lower().str.contains("jaciara", na=False)
            ]
            logger.info(f"  - {len(jaciara_desaprop)} em Jaciara")
            
            if not jaciara_desaprop.empty:
                for _, row in jaciara_desaprop.iterrows():
                    resultados.append({
                        "tipo": "SIGEF - Parcela em Desapropriação",
                        "codigo": row.get("codigo_imovel", "N/A"),
                        "municipio": row.get("municipio", "N/A"),
                        "area_ha": row.get("area_ha", 0),
                        "situacao": row.get("situacao", "N/A"),
                        "fonte": row.get("fonte", "INCRA/SIGEF"),
                        "data_coleta": row.get("coletado_em", datetime.now()),
                    })
        
        # Consulta tabela de desapropriações ativas do banco
        logger.info("→ Consultando desapropriações ativas registradas...")
        try:
            sql_desaprop = """
                SELECT codigo_imovel, municipio, area_ha, situacao, fonte, detectado_em, geometry
                FROM desapropriacao_ativa
                WHERE municipio ILIKE :municipio
                ORDER BY detectado_em DESC
            """
            df_desaprop = db.query(sql_desaprop, {"municipio": "%jaciara%"})
            
            if not df_desaprop.empty:
                logger.info(f"  - {len(df_desaprop)} registros de desapropriação ativa em Jaciara")
                
                for _, row in df_desaprop.iterrows():
                    resultados.append({
                        "tipo": "Desapropriação Ativa (Banco)",
                        "codigo": row.get("codigo_imovel", "N/A"),
                        "municipio": row.get("municipio", "N/A"),
                        "area_ha": row.get("area_ha", 0),
                        "situacao": row.get("situacao", "N/A"),
                        "fonte": row.get("fonte", "Sistema"),
                        "data_coleta": row.get("detectado_em", datetime.now()),
                    })
        except Exception as e:
            logger.warning(f"  ⚠ Erro ao consultar desapropriações ativas: {e}")
        
        # Consulta processos judiciais relacionados a desapropriação em Jaciara
        logger.info("→ Consultando processos judiciais de desapropriação...")
        try:
            sql_procs = """
                SELECT p.numero_cnj, p.classe_processual, p.assunto_principal, 
                       p.data_distribuicao, p.municipio, s.score_total, s.tipo_pericia_sugerida
                FROM processos p
                LEFT JOIN scores s ON p.id = s.processo_id
                WHERE (p.municipio ILIKE :municipio OR p.assunto_principal ILIKE :keyword)
                AND (p.classe_processual ILIKE :keyword OR p.assunto_principal ILIKE :keyword)
                AND EXTRACT(YEAR FROM p.data_distribuicao) = 2026
                ORDER BY p.data_distribuicao DESC
                LIMIT 20
            """
            df_procs = db.query(sql_procs, {
                "municipio": "%jaciara%",
                "keyword": "%desapropriacao%"
            })
            
            if not df_procs.empty:
                logger.info(f"  - {len(df_procs)} processos judiciais de desapropriação em 2026")
                
                for _, row in df_procs.iterrows():
                    resultados.append({
                        "tipo": "Processo Judicial - Desapropriação",
                        "codigo": row.get("numero_cnj", "N/A"),
                        "municipio": row.get("municipio", "N/A"),
                        "classe": row.get("classe_processual", "N/A"),
                        "assunto": row.get("assunto_principal", "N/A"),
                        "data_distribuicao": row.get("data_distribuicao", "N/A"),
                        "score_pericia": row.get("score_total", 0),
                        "tipo_pericia": row.get("tipo_pericia_sugerida", "N/A"),
                        "fonte": "DataJud/CNJ",
                    })
        except Exception as e:
            logger.warning(f"  ⚠ Erro ao consultar processos: {e}")
        
        # Consulta eventos administrativos (DOU, Portarias, etc)
        logger.info("→ Consultando eventos administrativos (DOU, Portarias)...")
        try:
            sql_eventos = """
                SELECT titulo, municipio, area_ha, data_publicacao, fonte, resumo
                FROM publicacoes
                WHERE municipio ILIKE :municipio
                AND (titulo ILIKE :keyword OR resumo ILIKE :keyword)
                AND EXTRACT(YEAR FROM data_publicacao) = 2026
                ORDER BY data_publicacao DESC
                LIMIT 20
            """
            df_eventos = db.query(sql_eventos, {
                "municipio": "%jaciara%",
                "keyword": "%desapropriacao%"
            })
            
            if not df_eventos.empty:
                logger.info(f"  - {len(df_eventos)} eventos administrativos de desapropriação em 2026")
                
                for _, row in df_eventos.iterrows():
                    resultados.append({
                        "tipo": "Evento Administrativo - DOU/Portaria",
                        "titulo": row.get("titulo", "N/A"),
                        "municipio": row.get("municipio", "N/A"),
                        "area_ha": row.get("area_ha", 0),
                        "data_publicacao": row.get("data_publicacao", "N/A"),
                        "fonte": row.get("fonte", "DOU"),
                        "resumo": row.get("resumo", "N/A"),
                    })
        except Exception as e:
            logger.warning(f"  ⚠ Erro ao consultar eventos: {e}")
        
        # Gera relatório em texto
        logger.info("→ Gerando relatório...")
        relatorio = gerar_relatorio(resultados)
        
        # Salva em arquivo txt na pasta base
        arquivo_saida = Path(__file__).parent / "DESAPROPIACAO_JACIARA_2026.txt"
        with open(arquivo_saida, "w", encoding="utf-8") as f:
            f.write(relatorio)
        
        logger.info(f"✓ Relatório salvo: {arquivo_saida}")
        logger.info(f"✓ Total de registros encontrados: {len(resultados)}")
        
        # Exibe resumo
        print("\n" + "="*80)
        print("RESUMO - DESAPROPRIAÇÕES DE TERRAS EM JACIARA, MT (2026)")
        print("="*80)
        print(f"Total de registros encontrados: {len(resultados)}")
        print(f"Arquivo salvo em: {arquivo_saida}")
        print("="*80 + "\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Erro na execução: {e}", exc_info=True)
        return 1


def gerar_relatorio(resultados):
    """
    Gera um relatório formatado em texto.
    """
    from datetime import datetime
    
    relatorio_linhas = []
    relatorio_linhas.append("="*100)
    relatorio_linhas.append("RELATÓRIO DE DESAPROPRIAÇÕES DE TERRAS - JACIARA, MATO GROSSO (MT)")
    relatorio_linhas.append("Período: 2026")
    relatorio_linhas.append(f"Data de Consulta: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    relatorio_linhas.append("="*100)
    relatorio_linhas.append("")
    relatorio_linhas.append(f"TOTAL DE REGISTROS ENCONTRADOS: {len(resultados)}")
    relatorio_linhas.append("")
    relatorio_linhas.append("-"*100)
    
    if not resultados:
        relatorio_linhas.append("Nenhum registro de desapropriação encontrado para Jaciara, MT em 2026.")
        relatorio_linhas.append("-"*100)
    else:
        # Agrupa por tipo
        por_tipo = {}
        for item in resultados:
            tipo = item.get("tipo", "N/A")
            if tipo not in por_tipo:
                por_tipo[tipo] = []
            por_tipo[tipo].append(item)
        
        # Exibe cada tipo de registro
        for idx_tipo, (tipo, items) in enumerate(por_tipo.items(), 1):
            relatorio_linhas.append(f"\n{idx_tipo}. {tipo}")
            relatorio_linhas.append(f"   Total: {len(items)} registro(s)")
            relatorio_linhas.append("   " + "-"*96)
            
            for idx, item in enumerate(items, 1):
                relatorio_linhas.append(f"\n   [{idx}] {tipo}")
                
                # Formata campos específicos para cada tipo
                if "SIGEF" in tipo or "Desapropriação Ativa" in tipo:
                    relatorio_linhas.append(f"       Código do Imóvel: {item.get('codigo', 'N/A')}")
                    relatorio_linhas.append(f"       Município: {item.get('municipio', 'N/A')}")
                    relatorio_linhas.append(f"       Área (hectares): {item.get('area_ha', 0):.2f}")
                    relatorio_linhas.append(f"       Situação: {item.get('situacao', 'N/A')}")
                    relatorio_linhas.append(f"       Fonte: {item.get('fonte', 'N/A')}")
                    if item.get("data_coleta"):
                        try:
                            data_str = item["data_coleta"].strftime('%d/%m/%Y') if hasattr(item["data_coleta"], 'strftime') else str(item["data_coleta"])
                            relatorio_linhas.append(f"       Data de Coleta: {data_str}")
                        except:
                            pass
                
                elif "Processo Judicial" in tipo:
                    relatorio_linhas.append(f"       Número CNJ: {item.get('codigo', 'N/A')}")
                    relatorio_linhas.append(f"       Município: {item.get('municipio', 'N/A')}")
                    relatorio_linhas.append(f"       Classe: {item.get('classe', 'N/A')}")
                    relatorio_linhas.append(f"       Assunto: {item.get('assunto', 'N/A')}")
                    relatorio_linhas.append(f"       Data de Distribuição: {item.get('data_distribuicao', 'N/A')}")
                    relatorio_linhas.append(f"       Score de Perícia: {item.get('score_pericia', 0)}/100")
                    relatorio_linhas.append(f"       Tipo de Perícia: {item.get('tipo_pericia', 'N/A')}")
                    relatorio_linhas.append(f"       Fonte: {item.get('fonte', 'N/A')}")
                
                elif "Evento Administrativo" in tipo:
                    relatorio_linhas.append(f"       Título: {item.get('titulo', 'N/A')}")
                    relatorio_linhas.append(f"       Município: {item.get('municipio', 'N/A')}")
                    if item.get("area_ha", 0) > 0:
                        relatorio_linhas.append(f"       Área (hectares): {item.get('area_ha', 0):.2f}")
                    relatorio_linhas.append(f"       Data de Publicação: {item.get('data_publicacao', 'N/A')}")
                    relatorio_linhas.append(f"       Fonte: {item.get('fonte', 'N/A')}")
                    if item.get("resumo"):
                        resumo = item.get("resumo", "")
                        if len(resumo) > 200:
                            resumo = resumo[:200] + "..."
                        relatorio_linhas.append(f"       Resumo: {resumo}")
            
            relatorio_linhas.append("\n   " + "-"*96)
    
    relatorio_linhas.append("\n" + "="*100)
    relatorio_linhas.append("FIM DO RELATÓRIO")
    relatorio_linhas.append("="*100)
    relatorio_linhas.append("\nNOTA: Este relatório foi gerado automaticamente pelo sistema Radar Pericial.")
    relatorio_linhas.append("Para mais informações, consulte: https://github.com/duafnso/radar-pericial")
    
    return "\n".join(relatorio_linhas)


if __name__ == "__main__":
    sys.exit(main())
