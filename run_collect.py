#!/usr/bin/env python3
"""
run_collect.py — Radar Pericial: orquestrador de coleta

Uso:
  python run_collect.py                          # tudo
  python run_collect.py --source demo            # dados offline (sem internet)
  python run_collect.py --source geo             # IBGE, SIGEF, FUNAI, INPE, CAR
  python run_collect.py --source judicial        # DataJud/CNJ, TJMT, DJe
  python run_collect.py --source admin           # DOU, IOMAT, DNIT, SINFRA-MT
  python run_collect.py --source score           # recalcula todos os scores
  python run_collect.py --source judicial admin --dias 60
"""

import argparse
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ── Geoespacial ───────────────────────────────────────────────────────
def run_geo():
    from database.db import Database
    from collector.multi_source_collector import MultiSourceCollector
    from etl.geospatial_etl import run_etl

    db  = Database()
    raw = MultiSourceCollector().run()
    cleaned = run_etl(raw, municipios=raw.get("municipios_mt"))
    db.save_all_layers(cleaned)

    sigef = cleaned.get("sigef_parcelas")
    if sigef is not None and hasattr(sigef, "empty") and not sigef.empty:
        if "desapropriacao_flag" in sigef.columns:
            ativas = sigef[sigef["desapropriacao_flag"] == True]
            if not ativas.empty:
                db.save_geodataframe(ativas, "desapropriacao_ativa")
                logger.info(f">>> {len(ativas)} áreas em desapropriação salvas")
    _stats(db)


# ── Judicial ──────────────────────────────────────────────────────────
def run_judicial(dias: int = 30):
    from database.db import Database
    from collector.judicial_collector import JudicialCollector

    db  = Database()
    res = JudicialCollector().run(dias_atras=dias)
    salvos = 0
    for proc in res.get("processos", []):
        score_dict = proc.pop("_score", {})
        movs       = proc.pop("_movimentacoes", [])
        pid = db.upsert_processo(proc)
        if pid and score_dict:
            db.save_score(pid, {**score_dict, "processo_id": pid})
        for mov in movs[:5]:
            db.save_movimentacao(pid, {
                "data_movimentacao": proc.get("data_distribuicao"),
                "descricao": mov,
                "fonte": proc.get("tribunal","judicial"),
                "score_evento": 0,
            })
        salvos += 1
    logger.info(f"Judicial: {salvos} processos salvos")
    _stats(db)


# ── Administrativo ────────────────────────────────────────────────────
def run_admin(dias: int = 30):
    from database.db import Database
    from collector.admin_collector import AdminCollector

    db     = Database()
    eventos = AdminCollector().run(dias_atras=dias)
    db.save_portarias(eventos)
    logger.info(f"Admin: {len(eventos)} eventos salvos")
    _stats(db)


# ── Recalcula scores ──────────────────────────────────────────────────
def run_score():
    from database.db import Database
    from intelligence.taxonomy import calcular_score

    db    = Database()
    procs = db.query("SELECT id, classe_processual, assunto_principal FROM processos")
    for _, row in procs.iterrows():
        movs = db.query(
            "SELECT descricao FROM movimentacoes WHERE processo_id=:id ORDER BY data_movimentacao DESC LIMIT 10",
            {"id": row["id"]},
        )["descricao"].tolist()
        pubs = db.query(
            "SELECT texto FROM publicacoes WHERE processo_id=:id ORDER BY data_publicacao DESC LIMIT 5",
            {"id": row["id"]},
        )["texto"].tolist()
        sc = calcular_score(
            classe_processual=row.get("classe_processual",""),
            assunto=row.get("assunto_principal",""),
            movimentacoes=movs, publicacoes=pubs,
        )
        db.save_score(row["id"], {**sc.to_dict(), "processo_id": row["id"]})
    logger.info(f"Score: {len(procs)} processos recalculados")


# ── Demo offline ──────────────────────────────────────────────────────
def run_demo():
    from database.db import Database
    from working_data_collector import WorkingDataCollector

    db        = Database()
    collector = WorkingDataCollector()

    # Usa populate_all que carrega os 20 tipos de conteúdo diretamente no banco
    try:
        collector.populate_all(db)
    except Exception as e:
        logger.error(f"populate_all falhou: {e} — tentando método legado")
        data = collector.create_realistic_data()
        data = collector.convert_to_geodataframes(data)
        if data.get("municipios") is not None:
            db.save_geodataframe(data["municipios"], "municipios_mt")
        if data.get("desapropriacao") is not None:
            desaprop = data["desapropriacao"].copy()
            desaprop["desapropriacao_flag"] = True
            desaprop["fonte"] = "DEMO/Offline"
            desaprop["coletado_em"] = datetime.utcnow()
            db.save_geodataframe(desaprop, "parcelas_sigef")

    logger.info("Demo carregado com sucesso.")
    _stats(db)


# ── Stats ─────────────────────────────────────────────────────────────
def _stats(db):
    s = db.stats()
    logger.info("─── Resumo ───────────────────────────────────────")
    logger.info(f"  Processos judiciais:   {s.get('total_processos', 0)}")
    logger.info(f"  🔥 Janelas quentes:    {s.get('processos_quentes', 0)}")
    logger.info(f"  ⚠️  Prováveis perícias: {s.get('processos_provaveis', 0)}")
    logger.info(f"  Desapropriações SIGEF: {s.get('total_desapropriadas', 0)}")
    logger.info(f"  Portarias DO:          {s.get('total_portarias', 0)}")
    logger.info(f"  Assentamentos INCRA:   {s.get('total_assentamentos', 0)}")
    logger.info(f"  Terras Indígenas:      {s.get('total_ti', 0)}")
    logger.info("──────────────────────────────────────────────────")


# ── CLI ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Radar Pericial — Coletor")
    parser.add_argument(
        "--source", "-s", nargs="+",
        choices=["all","geo","judicial","admin","score","demo"],
        default=["all"],
    )
    parser.add_argument("--dias", type=int, default=30, help="Dias atrás (padrão: 30)")
    args = parser.parse_args()
    sources = args.source

    if "demo" in sources:
        run_demo()
    else:
        if "all" in sources or "geo"      in sources: run_geo()
        if "all" in sources or "judicial" in sources: run_judicial(dias=args.dias)
        if "all" in sources or "admin"    in sources: run_admin(dias=args.dias)
        if "all" in sources or "score"    in sources: run_score()

    logger.info("=== Coleta concluída ===")
