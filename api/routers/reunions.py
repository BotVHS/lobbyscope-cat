"""Endpoints de reunions."""

import os
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from db.session import get_db_fastapi

router = APIRouter()

_PGVECTOR = os.getenv("PGVECTOR_ENABLED", "false").lower() == "true"


@router.get("/cerca")
def cerca_semantica(
    q: str = Query(..., min_length=3, max_length=500, description="Text a cercar"),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db_fastapi),
):
    """Cerca semàntica de reunions per similitud de tema. Requereix PGVECTOR_ENABLED=true."""
    if not _PGVECTOR:
        raise HTTPException(503, "Cerca semàntica no disponible (PGVECTOR_ENABLED=false)")

    from processament.embeddings import generar_embeddings_batch
    embeds = generar_embeddings_batch([q])
    if not embeds:
        raise HTTPException(500, "Error generant embedding de la consulta")

    embed_str = "[" + ",".join(str(x) for x in embeds[0]) + "]"

    rows = db.execute(text("""
        SELECT
            r.id, r.data_reunio, r.departament, r.activitat, r.tema_original,
            g.nom AS nom_grup, g.id AS grup_id,
            c.nom_canonical AS nom_carrec,
            ROUND((1 - (r.embedding_tema <=> :embed::vector))::numeric, 4) AS similitud
        FROM reunions r
        LEFT JOIN grups g   ON g.id = r.grup_id
        LEFT JOIN carrecs c ON c.id = r.carrec_id
        WHERE r.embedding_tema IS NOT NULL
        ORDER BY r.embedding_tema <=> :embed::vector
        LIMIT :limit
    """), {"embed": embed_str, "limit": limit}).fetchall()

    return {"query": q, "items": [dict(r._mapping) for r in rows]}


@router.get("/recent")
def reunions_recents(
    dies: int = Query(30, ge=1, le=365),
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db_fastapi),
):
    rows = db.execute(text("""
        SELECT
            r.id, r.font_id, r.data_reunio, r.departament,
            r.activitat, r.tema_original,
            g.nom AS nom_grup, g.id AS grup_id,
            c.nom_canonical AS nom_carrec, c.id AS carrec_id,
            COUNT(cx.id) AS num_connexions
        FROM reunions r
        LEFT JOIN grups g       ON g.id  = r.grup_id
        LEFT JOIN carrecs c     ON c.id  = r.carrec_id
        LEFT JOIN connexions cx ON cx.reunio_id = r.id
        WHERE r.data_reunio >= CURRENT_DATE - INTERVAL '1 day' * :dies
        GROUP BY r.id, r.font_id, r.data_reunio, r.departament,
                 r.activitat, r.tema_original, g.nom, g.id,
                 c.nom_canonical, c.id
        ORDER BY r.data_reunio DESC
        LIMIT :limit
    """), {"dies": dies, "limit": limit}).fetchall()

    return {"items": [dict(r._mapping) for r in rows]}


@router.get("/{reunio_id}")
def fitxa_reunio(reunio_id: int, db=Depends(get_db_fastapi)):
    row = db.execute(text("""
        SELECT
            r.id, r.font_id, r.data_reunio, r.departament,
            r.unitat_organica, r.activitat,
            r.tema_original, r.tema_normalitzat,
            g.id AS grup_id, g.nom AS nom_grup,
            g.codi_registre, g.ambit_interes,
            c.id AS carrec_id, c.nom_canonical AS nom_carrec,
            c.titol AS titol_carrec, c.tipologia
        FROM reunions r
        LEFT JOIN grups g   ON g.id = r.grup_id
        LEFT JOIN carrecs c ON c.id = r.carrec_id
        WHERE r.id = :id
    """), {"id": reunio_id}).fetchone()

    if not row:
        raise HTTPException(404, "Reunió no trobada")

    reunio = dict(row._mapping)

    # Connexions detectades per a aquesta reunió
    connexions = db.execute(text("""
        SELECT
            cx.id, cx.connexio_score, cx.tipus_decisio,
            cx.dies_entre_reunio_decisio, cx.similitud_semantica,
            cx.similitud_departament, cx.explicacio_ca, cx.factors_connexio,
            nd.titol AS titol_decisio, nd.data_publicacio AS data_decisio,
            nd.url_dogc, nd.tipus_norma
        FROM connexions cx
        LEFT JOIN normativa_dogc nd ON nd.id = cx.decisio_normativa_id
        WHERE cx.reunio_id = :id
        ORDER BY cx.connexio_score DESC
        LIMIT 10
    """), {"id": reunio_id}).fetchall()

    reunio["connexions"] = [dict(c._mapping) for c in connexions]
    return reunio
