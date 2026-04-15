"""
alerts/scheduler.py — Celery Beat
Agendamento automático de todas as coletas do Radar Pericial.
Horários em fuso America/Cuiaba.
"""

import logging
import os

from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

app = Celery("radar_pericial", broker=REDIS_URL, backend=REDIS_URL)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    timezone="America/Cuiaba",
    enable_utc=True,
    task_routes={
        "alerts.scheduler.task_geo":      {"queue": "geo"},
        "alerts.scheduler.task_judicial": {"queue": "judicial"},
        "alerts.scheduler.task_admin":    {"queue": "admin"},
        "alerts.scheduler.task_score":    {"queue": "default"},
        "alerts.scheduler.task_alerta":   {"queue": "default"},
    },
    beat_schedule={
        # Geoespacial — a cada 12 horas
        "coletar-geo": {
            "task": "alerts.scheduler.task_geo",
            "schedule": crontab(minute=0, hour="*/12"),
        },
        # Processos judiciais — todo dia às 06:00
        "coletar-judicial": {
            "task": "alerts.scheduler.task_judicial",
            "schedule": crontab(minute=0, hour=6),
            "kwargs": {"dias_atras": 1},
        },
        # Eventos administrativos — todo dia às 07:00
        "coletar-admin": {
            "task": "alerts.scheduler.task_admin",
            "schedule": crontab(minute=0, hour=7),
            "kwargs": {"dias_atras": 2},
        },
        # Recalcula scores — todo dia às 08:00
        "recalcular-scores": {
            "task": "alerts.scheduler.task_score",
            "schedule": crontab(minute=0, hour=8),
        },
    },
)


# ── Tarefa: coleta geoespacial ─────────────────────────────────────────
@app.task(bind=True, max_retries=2, default_retry_delay=600, queue="geo")
def task_geo(self):
    try:
        from database.db import Database
        from collector.multi_source_collector import MultiSourceCollector
        from etl.geospatial_etl import clean_gdf, run_etl
        import geopandas as gpd

        db  = Database()
        raw = MultiSourceCollector().run()

        # Fix: limpa municipios_mt ANTES de passá-los como referência no ETL.
        # Anteriormente, o GDF cru (com possíveis geometrias inválidas e CRS errado)
        # era usado como referência para enriquecer os demais layers — causando
        # falhas silenciosas no sjoin do enrich_municipio.
        municipios_clean = None
        mun_raw = raw.get("municipios_mt")
        if mun_raw is not None and isinstance(mun_raw, gpd.GeoDataFrame) and not mun_raw.empty:
            municipios_clean = clean_gdf(mun_raw, name="municipios_mt")

        cleaned = run_etl(raw, municipios=municipios_clean)
        db.save_all_layers(cleaned)

        sigef = cleaned.get("sigef_parcelas")
        if sigef is not None and hasattr(sigef, "empty") and not sigef.empty:
            if "desapropriacao_flag" in sigef.columns:
                ativas = sigef[sigef["desapropriacao_flag"] == True]
                if not ativas.empty:
                    db.save_geodataframe(ativas, "desapropriacao_ativa")

        logger.info("task_geo concluída")
        return {"status": "ok", "task": "geo"}
    except Exception as e:
        logger.error(f"task_geo: {e}")
        raise self.retry(exc=e)


# ── Tarefa: coleta judicial ────────────────────────────────────────────
@app.task(bind=True, max_retries=2, default_retry_delay=300, queue="judicial")
def task_judicial(self, dias_atras: int = 1):
    try:
        from database.db import Database
        from collector.judicial_collector import JudicialCollector

        db  = Database()
        res = JudicialCollector().run(dias_atras=dias_atras)

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
                    "fonte": proc.get("tribunal", "judicial"),
                    "score_evento": 0,
                })
            salvos += 1

        # Verifica janelas quentes e dispara alerta
        quentes = db.get_processos_quentes(faixa="janela_quente", limit=5)
        if not quentes.empty:
            task_alerta.delay("judicial", quentes.to_dict("records"))

        logger.info(f"task_judicial: {salvos} processos")
        return {"status": "ok", "salvos": salvos}
    except Exception as e:
        logger.error(f"task_judicial: {e}")
        raise self.retry(exc=e)


# ── Tarefa: eventos administrativos ───────────────────────────────────
@app.task(bind=True, max_retries=2, default_retry_delay=300, queue="admin")
def task_admin(self, dias_atras: int = 2):
    try:
        from database.db import Database
        from collector.admin_collector import AdminCollector

        db     = Database()
        eventos = AdminCollector().run(dias_atras=dias_atras)
        db.save_portarias(eventos)

        quentes = [e for e in eventos if e.get("faixa_probabilidade") == "janela_quente"]
        if quentes:
            task_alerta.delay("admin", quentes[:5])

        logger.info(f"task_admin: {len(eventos)} eventos")
        return {"status": "ok", "eventos": len(eventos)}
    except Exception as e:
        logger.error(f"task_admin: {e}")
        raise self.retry(exc=e)


# ── Tarefa: recalcula scores ───────────────────────────────────────────
@app.task(bind=True, max_retries=1, queue="default")
def task_score(self):
    try:
        from database.db import Database
        from intelligence.taxonomy import calcular_score

        db = Database()
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
            score = calcular_score(
                classe_processual=row.get("classe_processual",""),
                assunto=row.get("assunto_principal",""),
                movimentacoes=movs,
                publicacoes=pubs,
            )
            db.save_score(row["id"], {**score.to_dict(), "processo_id": row["id"]})

        logger.info(f"task_score: {len(procs)} processos recalculados")
        return {"status": "ok", "recalculados": len(procs)}
    except Exception as e:
        logger.error(f"task_score: {e}")
        raise self.retry(exc=e)


# ── Tarefa: envio de alertas ───────────────────────────────────────────
@app.task(queue="default")
def task_alerta(origem: str, items: list):
    telegram_token  = os.getenv("TELEGRAM_BOT_TOKEN")
    telegram_chat   = os.getenv("TELEGRAM_CHAT_ID")
    webhook_url     = os.getenv("WEBHOOK_ALERT_URL")

    if not items:
        return

    linhas = []
    for item in items[:5]:
        mun   = item.get("municipio") or item.get("comarca", "?")
        score = item.get("score_total") or item.get("score_evento", "?")
        tipo  = item.get("tipo_pericia_sugerida") or item.get("categoria_agronomica", "")
        linhas.append(f"• {mun} | Score {score} | {tipo}")

    emoji = "⚖️" if origem == "judicial" else "📋"
    msg = (
        f"🔥 *Radar Pericial — {origem.upper()} — MT*\n\n"
        f"{emoji} {len(items)} evento(s) com alta probabilidade de perícia:\n"
        + "\n".join(linhas)
    )

    import requests as req

    if telegram_token and telegram_chat:
        try:
            req.post(
                f"https://api.telegram.org/bot{telegram_token}/sendMessage",
                json={"chat_id": telegram_chat, "text": msg, "parse_mode": "Markdown"},
                timeout=10,
            )
            logger.info("Alerta Telegram enviado")
        except Exception as e:
            logger.error(f"Telegram: {e}")

    if webhook_url:
        try:
            req.post(webhook_url, json={"text": msg}, timeout=10)
            logger.info("Alerta webhook enviado")
        except Exception as e:
            logger.error(f"Webhook: {e}")
