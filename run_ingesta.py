"""
Script principal d'ingesta. Executa el pipeline complet o fases individuals.

Ús:
    python run_ingesta.py               # tot el pipeline
    python run_ingesta.py --fase agendes
    python run_ingesta.py --fase grups
    python run_ingesta.py --fase dogc
    python run_ingesta.py --fase subvencions     # RAISC s9xt-n979 → subvencions_lobby
    python run_ingesta.py --fase contractes      # hb6v-jcbf → contractes_lobby
    python run_ingesta.py --fase acords_govern   # ub8p-uqwj → acords_govern
    python run_ingesta.py --fase embeddings
    python run_ingesta.py --fase connexions
    python run_ingesta.py --fase scores
    python run_ingesta.py --fase classificar     # requereix ANTHROPIC_API_KEY
    python run_ingesta.py --fase stats           # mostra recomptes de la BD

ORDRE RECOMANAT PER A PRIMERA INGESTA COMPLETA:
    1. python run_ingesta.py --fase agendes
    2. python run_ingesta.py --fase grups
    3. python run_ingesta.py --fase dogc
    4. python run_ingesta.py --fase acords_govern
    5. psql $DATABASE_URL -f db/create_vector_indexes.sql   # crear índexs ivfflat
    6. python run_ingesta.py --fase embeddings
    7. python run_ingesta.py --fase connexions
    8. python run_ingesta.py --fase scores
    9. python run_ingesta.py --fase classificar  # opcional
   10. python run_ingesta.py --fase stats
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_ingesta")


def run_agendes(db):
    from ingesta.agendes import ingestar_agendes
    logger.info("=== FASE: Agendes (hd8k-y28e) ===")
    return ingestar_agendes(db)


def run_grups(db):
    from ingesta.grups_detall import enriquir_grups
    logger.info("=== FASE: Enriquiment grups (gwpn-de62) ===")
    return enriquir_grups(db)


def run_dogc(db):
    from ingesta.dogc import ingestar_normativa_dogc
    logger.info("=== FASE: Normativa DOGC (n6hn-rmy7) ===")
    return ingestar_normativa_dogc(db)


def run_subvencions(db):
    from ingesta.subvencions import ingestar_subvencions
    logger.info("=== FASE: Subvencions RAISC (s9xt-n979) ===")
    stats = ingestar_subvencions(db)
    logger.info(f"Subvencions: {stats}")
    return stats


def run_contractes(db):
    from ingesta.contractes import ingestar_contractes
    logger.info("=== FASE: Contractes públics (hb6v-jcbf) ===")
    stats = ingestar_contractes(db)
    logger.info(f"Contractes: {stats}")
    return stats


def run_acords_govern(db):
    from ingesta.acords_govern import ingestar_acords_govern
    logger.info("=== FASE: Acords del Govern (ub8p-uqwj) ===")
    stats = ingestar_acords_govern(db)
    logger.info(f"Acords del Govern: {stats}")
    return stats


def _run_embeddings(db):
    from processament.embeddings import actualitzar_tots_embeddings
    logger.info("=== FASE: Embeddings (sentence-transformers local) ===")
    stats = actualitzar_tots_embeddings(db)
    logger.info(f"Embeddings: {stats}")


def _run_connexions(db):
    from processament.detector_connexions import detectar_totes_connexions
    logger.info("=== FASE: Detecció de connexions ===")
    stats = detectar_totes_connexions(db)
    logger.info(f"Connexions: {stats}")


def _run_scores(db):
    from processament.scores import recalcular_tots_scores
    logger.info("=== FASE: Càlcul Lobby Influence Scores ===")
    stats = recalcular_tots_scores(db)
    logger.info(f"Scores: {stats}")


def _run_classificar(db):
    from processament.classificador import classificar_connexions_pendents
    logger.info("=== FASE: Classificació LLM de connexions (requereix ANTHROPIC_API_KEY) ===")
    stats = classificar_connexions_pendents(db)
    logger.info(f"Classificar: {stats}")


def run_stats(db):
    from sqlalchemy import text
    taules = [
        "carrecs", "grups", "reunions", "normativa_dogc",
        "acords_govern", "subvencions_lobby", "contractes_lobby", "connexions",
    ]
    logger.info("=== ESTADÍSTIQUES DE LA BD ===")
    for taula in taules:
        try:
            n = db.execute(text(f"SELECT COUNT(*) FROM {taula}")).scalar()
            logger.info(f"  {taula:<25} {n:>8} registres")
        except Exception as e:
            logger.warning(f"  {taula:<25} ERROR: {e}")

    # Reunions amb embedding
    emb = db.execute(text(
        "SELECT COUNT(*) FROM reunions WHERE embedding_tema IS NOT NULL"
    )).scalar()
    logger.info(f"  {'reunions amb embedding':<25} {emb:>8}")


def main():
    parser = argparse.ArgumentParser(description="Pipeline d'ingesta de lobbyscope.cat")
    parser.add_argument(
        "--fase",
        choices=["agendes", "grups", "dogc", "subvencions", "contractes",
                 "acords_govern", "embeddings", "connexions", "scores",
                 "classificar", "stats", "tot"],
        default="tot",
        help="Fase a executar (default: tot)",
    )
    args = parser.parse_args()

    from db.session import get_db

    with get_db() as db:
        if args.fase == "agendes":
            run_agendes(db)
        elif args.fase == "grups":
            run_grups(db)
        elif args.fase == "dogc":
            run_dogc(db)
        elif args.fase == "subvencions":
            run_subvencions(db)
        elif args.fase == "contractes":
            run_contractes(db)
        elif args.fase == "acords_govern":
            run_acords_govern(db)
        elif args.fase == "embeddings":
            _run_embeddings(db)
        elif args.fase == "connexions":
            _run_connexions(db)
        elif args.fase == "scores":
            _run_scores(db)
        elif args.fase == "classificar":
            _run_classificar(db)
        elif args.fase == "stats":
            run_stats(db)
        else:  # tot
            run_agendes(db)
            run_grups(db)
            run_dogc(db)
            run_subvencions(db)
            run_contractes(db)
            run_acords_govern(db)
            _run_embeddings(db)
            _run_connexions(db)
            _run_scores(db)
            run_stats(db)

    logger.info("Pipeline completat.")


if __name__ == "__main__":
    main()
