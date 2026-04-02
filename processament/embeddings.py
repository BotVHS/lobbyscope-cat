"""
Pipeline d'embeddings locals amb sentence-transformers.

MODEL: LaBSE (Language-Agnostic BERT Sentence Embedding)
  - 768 dimensions
  - Multilingüe (suport excel·lent per al català)
  - ~1.8 GB en disc (descarrega automàtica HuggingFace al primer ús)
  - Velocitat: ~500 frases/segon en CPU modern

ALTERNATIVA MÉS LLEUGERA:
  EMBEDDING_MODEL=sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2
  (384 dims — requereix ajustar vector(384) al schema si es canvia)

COST: zero (model local, sense API)

ESTRATÈGIA DE GENERACIÓ:
  - Batch de 64 textos per crida al model (equilibri memòria/velocitat)
  - Processar en ordre descendent per data (els més recents primer)
  - Regenerar NOMÉS quan el text font ha canviat (embedding IS NULL)
  - Guardar com a pgvector al PostgreSQL
"""

import logging
import os
import time
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/LaBSE")
EMBEDDING_DIMS  = int(os.getenv("EMBEDDING_DIMS", "768"))
BATCH_SIZE      = 64


@lru_cache(maxsize=1)
def _get_model():
    """
    Carrega el model de sentence-transformers (singleton amb cache).
    El primer accés descarrega el model (~1.8 GB) si no existeix localment.
    """
    from sentence_transformers import SentenceTransformer
    logger.info(f"Carregant model d'embeddings: {EMBEDDING_MODEL}")
    logger.info("(Primera execució: descarrega ~1.8 GB de HuggingFace)")
    model = SentenceTransformer(EMBEDDING_MODEL)
    logger.info("Model carregat.")
    return model


def generar_embedding(text: str) -> list[float]:
    """Genera l'embedding d'un text. Retorna vector de zeros si buit."""
    if not text or not text.strip():
        return [0.0] * EMBEDDING_DIMS
    try:
        model = _get_model()
        vector = model.encode(text.strip(), normalize_embeddings=True)
        return vector.tolist()
    except Exception as e:
        logger.error(f"Error generant embedding: {e}")
        return [0.0] * EMBEDDING_DIMS


def generar_embeddings_batch(textos: list[str]) -> list[list[float]]:
    """
    Genera embeddings en batch. Més eficient que cridar generar_embedding()
    de forma iterativa.
    """
    if not textos:
        return []
    try:
        model = _get_model()
        textos_nets = [t.strip() if t else "" for t in textos]
        vectors = model.encode(
            textos_nets,
            batch_size=BATCH_SIZE,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        return [v.tolist() for v in vectors]
    except Exception as e:
        logger.error(f"Error batch embeddings: {e}")
        # Fallback un per un
        return [generar_embedding(t) for t in textos]


def actualitzar_tots_embeddings(db) -> dict:
    """
    Genera embeddings per a tots els registres que no en tenen.
    Segur per executar múltiples vegades (idempotent).
    """
    stats = {}
    stats["reunions"]      = _embeddings_reunions(db)
    stats["dogc"]          = _embeddings_dogc(db)
    stats["grups"]         = _embeddings_grups(db)
    stats["subvencions"]   = _embeddings_subvencions(db)
    stats["contractes"]    = _embeddings_contractes(db)
    stats["acords_govern"] = _embeddings_acords_govern(db)
    return stats


def _embeddings_reunions(db) -> int:
    """
    Embeddings dels temes de les reunions. Dues passades:
    1. Reunions amb tema_normalitzat ≥ 15 chars (qualitat òptima)
    2. Reunions sense tema o massa curt → fallback amb tema_original +
       activitat + unitat_organica (cobrir "Reunió de treball", etc.)
    """
    from sqlalchemy import text

    # --- Passada 1: tema_normalitzat complet ---
    rows = db.execute(text("""
        SELECT id, tema_normalitzat AS text_embedding
        FROM reunions
        WHERE embedding_tema IS NULL
          AND tema_normalitzat IS NOT NULL
          AND LENGTH(tema_normalitzat) >= 15
        ORDER BY data_reunio DESC
    """)).fetchall()

    total = 0
    if rows:
        logger.info(f"[embeddings] Reunions (tema): {len(rows)} registres...")
        total += _processar_batch(
            db, rows,
            text_col="text_embedding",
            update_sql="UPDATE reunions SET embedding_tema = :embed WHERE id = :id",
            label="reunions",
        )

    # --- Passada 2: fallback per a reunions sense tema útil ---
    rows_fb = db.execute(text("""
        SELECT id,
               TRIM(
                   COALESCE(NULLIF(tema_original, ''), '') || ' ' ||
                   COALESCE(NULLIF(activitat, ''), '') || ' ' ||
                   COALESCE(NULLIF(unitat_organica, ''), '')
               ) AS text_embedding
        FROM reunions
        WHERE embedding_tema IS NULL
          AND (tema_normalitzat IS NULL OR LENGTH(tema_normalitzat) < 15)
          AND LENGTH(TRIM(
                  COALESCE(tema_original, '') || ' ' ||
                  COALESCE(activitat, '') || ' ' ||
                  COALESCE(unitat_organica, '')
              )) >= 10
        ORDER BY data_reunio DESC
    """)).fetchall()

    if rows_fb:
        logger.info(f"[embeddings] Reunions (fallback): {len(rows_fb)} registres...")
        total += _processar_batch(
            db, rows_fb,
            text_col="text_embedding",
            update_sql="UPDATE reunions SET embedding_tema = :embed WHERE id = :id",
            label="reunions-fallback",
        )
    elif not rows:
        logger.info("[embeddings] Reunions: ja al dia.")

    return total


def _embeddings_dogc(db) -> int:
    """Embeddings del títol i resum de les normes DOGC."""
    from sqlalchemy import text

    rows = db.execute(text("""
        SELECT id, titol, resum
        FROM normativa_dogc
        WHERE embedding_titol IS NULL
          AND titol IS NOT NULL
        ORDER BY data_publicacio DESC
    """)).fetchall()

    if not rows:
        logger.info("[embeddings] DOGC: ja al dia.")
        return 0

    logger.info(f"[embeddings] Generant embeddings per a {len(rows)} normes DOGC...")
    count = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        ids         = [r.id for r in batch]
        textos_tit  = [r.titol or "" for r in batch]
        textos_res  = [r.resum or r.titol or "" for r in batch]

        emb_titols = generar_embeddings_batch(textos_tit)
        emb_resums = generar_embeddings_batch(textos_res)

        for row_id, et, er in zip(ids, emb_titols, emb_resums):
            db.execute(text("""
                UPDATE normativa_dogc
                SET embedding_titol = :et, embedding_resum = :er
                WHERE id = :id
            """), {"et": et, "er": er, "id": row_id})

        db.commit()
        count += len(batch)
        if count % 2000 == 0:
            logger.info(f"  DOGC: {count}/{len(rows)}")

    logger.info(f"[embeddings] DOGC completat: {count} normes.")
    return count


def _embeddings_grups(db) -> int:
    """Embeddings dels objectius dels grups d'interès."""
    from sqlalchemy import text

    rows = db.execute(text("""
        SELECT id, nom, objectius
        FROM grups
        WHERE embedding_objectius IS NULL
    """)).fetchall()

    if not rows:
        logger.info("[embeddings] Grups: ja al dia.")
        return 0

    logger.info(f"[embeddings] Generant embeddings per a {len(rows)} grups...")
    # Text = nom + objectius per a representació semàntica completa
    rows_augmented = []
    for r in rows:
        text_complet = r.nom or ""
        if r.objectius:
            text_complet += ". " + r.objectius
        rows_augmented.append((r.id, text_complet))

    total = _processar_batch(
        db,
        [type("R", (), {"id": rid, "text": t})() for rid, t in rows_augmented],
        text_col="text",
        update_sql="UPDATE grups SET embedding_objectius = :embed WHERE id = :id",
        label="grups",
    )
    return total


def _embeddings_subvencions(db) -> int:
    """Embeddings de la finalitat de les subvencions vinculades a grups."""
    from sqlalchemy import text

    rows = db.execute(text("""
        SELECT id, finalitat AS text_embedding
        FROM subvencions_lobby
        WHERE embedding_finalitat IS NULL
          AND finalitat IS NOT NULL AND LENGTH(finalitat) >= 10
        ORDER BY data_concessio DESC
    """)).fetchall()

    if not rows:
        logger.info("[embeddings] Subvencions: ja al dia.")
        return 0

    logger.info(f"[embeddings] Generant embeddings per a {len(rows)} subvencions...")
    return _processar_batch(
        db, rows,
        text_col="text_embedding",
        update_sql="UPDATE subvencions_lobby SET embedding_finalitat = :embed WHERE id = :id",
        label="subvencions",
    )


def _embeddings_contractes(db) -> int:
    """Embeddings de l'objecte dels contractes vinculats a grups."""
    from sqlalchemy import text

    rows = db.execute(text("""
        SELECT id, objecte_contracte AS text_embedding
        FROM contractes_lobby
        WHERE embedding_objecte IS NULL
          AND objecte_contracte IS NOT NULL AND LENGTH(objecte_contracte) >= 10
        ORDER BY data_adjudicacio DESC
    """)).fetchall()

    if not rows:
        logger.info("[embeddings] Contractes: ja al dia.")
        return 0

    logger.info(f"[embeddings] Generant embeddings per a {len(rows)} contractes...")
    return _processar_batch(
        db, rows,
        text_col="text_embedding",
        update_sql="UPDATE contractes_lobby SET embedding_objecte = :embed WHERE id = :id",
        label="contractes",
    )


def _embeddings_acords_govern(db) -> int:
    """Embeddings del títol dels Acords del Govern."""
    from sqlalchemy import text

    rows = db.execute(text("""
        SELECT id, titol AS text_embedding
        FROM acords_govern
        WHERE embedding_titol IS NULL
          AND titol IS NOT NULL AND LENGTH(titol) >= 10
        ORDER BY data_sessio DESC
    """)).fetchall()

    if not rows:
        logger.info("[embeddings] Acords del Govern: ja al dia.")
        return 0

    logger.info(f"[embeddings] Generant embeddings per a {len(rows)} acords del govern...")
    return _processar_batch(
        db, rows,
        text_col="text_embedding",
        update_sql="UPDATE acords_govern SET embedding_titol = :embed WHERE id = :id",
        label="acords_govern",
    )


def _processar_batch(db, rows, text_col: str, update_sql: str, label: str) -> int:
    """Helper genèric per processar qualsevol taula en batches."""
    from sqlalchemy import text as sql_text

    count = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        ids    = [r.id for r in batch]
        textos = [getattr(r, text_col) or "" for r in batch]

        embeddings = generar_embeddings_batch(textos)

        for row_id, embed in zip(ids, embeddings):
            db.execute(sql_text(update_sql), {"embed": embed, "id": row_id})

        db.commit()
        count += len(batch)
        if count % 1000 == 0 or count == len(rows):
            logger.info(f"  {label}: {count}/{len(rows)}")

    return count
