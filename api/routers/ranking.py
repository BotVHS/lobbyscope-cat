"""Endpoints de rànquings."""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from db.session import get_db_fastapi

router = APIRouter()


@router.get("/grups")
def ranking_grups(
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db_fastapi),
):
    rows = db.execute(text("""
        SELECT
            g.id, g.nom, g.codi_registre, g.ambit_interes,
            ls.score_total,
            ls.score_frequencia, ls.score_diversitat_carrecs,
            ls.score_connexio_decisions, ls.score_valor_economic,
            ls.total_reunions, ls.total_connexions,
            ls.import_total_rebut, ls.ultima_reunio
        FROM grups g
        JOIN lobby_scores ls ON ls.grup_id = g.id
        ORDER BY ls.score_total DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()

    return {"items": [dict(r._mapping) for r in rows]}


@router.get("/carrecs")
def ranking_carrecs(
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db_fastapi),
):
    rows = db.execute(text("""
        SELECT
            c.id, c.nom_canonical, c.titol, c.departament,
            COUNT(r.id)               AS total_reunions,
            COUNT(DISTINCT r.grup_id) AS lobbies_contactats,
            MAX(r.data_reunio)        AS ultima_reunio
        FROM carrecs c
        JOIN reunions r ON r.carrec_id = c.id
        GROUP BY c.id, c.nom_canonical, c.titol, c.departament
        ORDER BY total_reunions DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()

    return {"items": [dict(r._mapping) for r in rows]}


@router.get("/connexions")
def ranking_connexions(
    score_min: float = Query(70.0, ge=0, le=100),
    limit: int = Query(50, ge=1, le=200),
    db=Depends(get_db_fastapi),
):
    rows = db.execute(text("""
        SELECT
            cx.id, cx.connexio_score, cx.tipus_decisio,
            cx.dies_entre_reunio_decisio, cx.similitud_semantica,
            cx.explicacio_ca,
            r.data_reunio, r.tema_original,
            g.nom AS nom_grup, g.id AS grup_id,
            c.nom_canonical AS nom_carrec,
            nd.titol AS titol_decisio,
            nd.data_publicacio AS data_decisio
        FROM connexions cx
        JOIN reunions r       ON r.id  = cx.reunio_id
        LEFT JOIN grups g     ON g.id  = r.grup_id
        LEFT JOIN carrecs c   ON c.id  = r.carrec_id
        LEFT JOIN normativa_dogc nd ON nd.id = cx.decisio_normativa_id
        WHERE cx.connexio_score >= :score_min
        ORDER BY cx.connexio_score DESC
        LIMIT :limit
    """), {"score_min": score_min, "limit": limit}).fetchall()

    return {"items": [dict(r._mapping) for r in rows]}
