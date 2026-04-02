"""
Scheduler setmanal per a lobbyscope.cat.

ORDRE D'EXECUCIÓ (cada diumenge a les 02:00 AM, hora de Madrid):
  1. Descarregar noves reunions (hd8k-y28e)
  2. Enriquir grups nous (gwpn-de62)
  3. Descarregar nova normativa DOGC (n6hn-rmy7)
  4. Generar embeddings per a nous registres
  5. Detectar connexions per a noves reunions
  6. Recalcular scores dels grups afectats
  7. Classificar LLM les noves connexions d'alt score (>= 65)
  8. Generar alertes de novetats
  9. Refrescar vistes materialitzades
  10. Enviar resum per webhook (opcional)

PER ARRENCAR:
  python -m ingesta.scheduler

PER EXECUTAR UNA VEGADA (debug):
  python -m ingesta.scheduler --run-now
"""

import argparse
import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scheduler")

scheduler = BlockingScheduler(timezone="Europe/Madrid")


@scheduler.scheduled_job("cron", day_of_week="sun", hour=2, minute=0, id="update_setmanal")
def actualitzacio_setmanal():
    """Pipeline complet d'actualització setmanal."""
    logger.info("=" * 60)
    logger.info("INICI ACTUALITZACIÓ SETMANAL")
    logger.info("=" * 60)

    from db.session import get_db
    from ingesta.agendes import ingestar_agendes
    from ingesta.grups_detall import enriquir_grups
    from ingesta.dogc import ingestar_normativa_dogc
    from processament.embeddings import actualitzar_tots_embeddings
    from processament.detector_connexions import detectar_totes_connexions
    from processament.scores import recalcular_tots_scores
    from processament.classificador import classificar_connexions_pendents
    from sqlalchemy import text

    resultats = {}

    with get_db() as db:
        try:
            logger.info("[1/9] Ingestant agendes...")
            resultats["agendes"] = ingestar_agendes(db)

            logger.info("[2/9] Enriquint grups...")
            resultats["grups"] = enriquir_grups(db)

            logger.info("[3/9] Ingestant DOGC...")
            resultats["dogc"] = ingestar_normativa_dogc(db)

            logger.info("[4/9] Generant embeddings nous...")
            resultats["embeddings"] = actualitzar_tots_embeddings(db)

            logger.info("[5/9] Detectant connexions...")
            resultats["connexions"] = detectar_totes_connexions(db)

            logger.info("[6/9] Recalculant scores...")
            resultats["scores"] = recalcular_tots_scores(db)

            logger.info("[7/9] Classificant amb LLM...")
            try:
                resultats["classificacio"] = classificar_connexions_pendents(db)
            except ValueError as e:
                logger.warning(f"Classificació LLM desactivada: {e}")

            logger.info("[8/9] Generant alertes...")
            _generar_alertes(db, resultats)

            logger.info("[9/9] Refrescant vistes materialitzades...")
            db.execute(text("REFRESH MATERIALIZED VIEW top_lobbies"))
            db.execute(text("REFRESH MATERIALIZED VIEW top_carrecs_reunions"))
            db.commit()

            logger.info("=" * 60)
            logger.info("ACTUALITZACIÓ SETMANAL COMPLETADA")
            logger.info(f"Resultats: {resultats}")
            logger.info("=" * 60)

            _enviar_resum(resultats)

        except Exception as e:
            logger.error(f"ERROR en l'actualització setmanal: {e}", exc_info=True)
            db.rollback()


def _generar_alertes(db, resultats: dict) -> None:
    """Genera alertes de les novetats de la setmana."""
    from sqlalchemy import text

    noves_reunions = (resultats.get("agendes") or {}).get("nous", 0)
    noves_connexions = (resultats.get("connexions") or {}).get("connexions", 0)

    if noves_reunions > 0:
        db.execute(text("""
            INSERT INTO alertes (tipus, descripcio)
            VALUES ('nova_reunio', :desc)
        """), {"desc": f"S'han afegit {noves_reunions} noves reunions aquesta setmana."})

    if noves_connexions > 0:
        db.execute(text("""
            INSERT INTO alertes (tipus, descripcio)
            VALUES ('nova_connexio', :desc)
        """), {"desc": f"S'han detectat {noves_connexions} noves connexions lobbies↔decisions."})

    db.commit()


def _enviar_resum(resultats: dict) -> None:
    """Envia el resum per webhook (opcional, configurat via WEBHOOK_URL)."""
    webhook_url = os.getenv("WEBHOOK_URL", "")
    if not webhook_url:
        return

    import requests
    try:
        msg = (
            "📊 *lobbyscope.cat — Actualització setmanal*\n"
            f"• Reunions noves: {(resultats.get('agendes') or {}).get('nous', 0)}\n"
            f"• Connexions detectades: {(resultats.get('connexions') or {}).get('connexions', 0)}\n"
            f"• Scores recalculats: {(resultats.get('scores') or {}).get('processats', 0)}\n"
        )
        requests.post(webhook_url, json={"text": msg}, timeout=10)
    except Exception as e:
        logger.warning(f"No s'ha pogut enviar el resum per webhook: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Executa el pipeline immediatament (per a debugging)",
    )
    args = parser.parse_args()

    if args.run_now:
        logger.info("Execució immediata del pipeline (--run-now)")
        actualitzacio_setmanal()
    else:
        logger.info("Scheduler actiu. Pròxima execució: diumenge 02:00 AM")
        scheduler.start()


if __name__ == "__main__":
    main()
