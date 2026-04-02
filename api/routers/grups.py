"""Endpoints de grups d'interès (lobbies)."""

from typing import Literal, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from db.session import get_db_fastapi

router = APIRouter()


@router.get("")
def llista_grups(
    q: Optional[str] = Query(None, description="Cerca per nom"),
    departament: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    ordenar: Literal["score", "reunions", "recent"] = Query("score"),
    db=Depends(get_db_fastapi),
):
    offset = (page - 1) * limit

    order_map = {
        "score":    "ls.score_total DESC NULLS LAST",
        "reunions": "ls.total_reunions DESC NULLS LAST",
        "recent":   "ls.ultima_reunio DESC NULLS LAST",
    }
    order_sql = order_map[ordenar]

    filters = []
    params: dict = {"limit": limit, "offset": offset}

    if q:
        filters.append("g.nom ILIKE :q")
        params["q"] = f"%{q}%"

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = db.execute(text(f"""
        SELECT
            g.id, g.nom, g.codi_registre, g.ambit_interes,
            g.situacio_inscripcio,
            ls.score_total,
            ls.total_reunions,
            ls.total_connexions,
            ls.ultima_reunio
        FROM grups g
        LEFT JOIN lobby_scores ls ON ls.grup_id = g.id
        {where}
        ORDER BY {order_sql}
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    total = db.execute(text(f"""
        SELECT COUNT(*) FROM grups g {where}
    """), {k: v for k, v in params.items() if k not in ("limit", "offset")}).scalar()

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "items": [dict(r._mapping) for r in rows],
    }


@router.get("/{grup_id}")
def fitxa_grup(grup_id: int, db=Depends(get_db_fastapi)):
    row = db.execute(text("""
        SELECT
            g.id, g.nom, g.codi_registre, g.cif,
            g.ambit_interes, g.objectius, g.situacio_inscripcio,
            g.primera_reunio,
            ls.score_total, ls.score_frequencia, ls.score_diversitat_carrecs,
            ls.score_connexio_decisions, ls.score_valor_economic,
            ls.total_reunions, ls.total_carrecs_contactats,
            ls.total_connexions, ls.import_total_rebut,
            ls.ultima_reunio
        FROM grups g
        LEFT JOIN lobby_scores ls ON ls.grup_id = g.id
        WHERE g.id = :id
    """), {"id": grup_id}).fetchone()

    if not row:
        raise HTTPException(404, "Grup no trobat")
    return dict(row._mapping)


@router.get("/{grup_id}/reunions")
def reunions_grup(
    grup_id: int,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db_fastapi),
):
    offset = (page - 1) * limit
    rows = db.execute(text("""
        SELECT
            r.id, r.font_id, r.data_reunio, r.departament,
            r.activitat, r.tema_original,
            c.nom_canonical AS nom_carrec, c.titol AS titol_carrec,
            COUNT(cx.id) AS num_connexions
        FROM reunions r
        LEFT JOIN carrecs c  ON c.id = r.carrec_id
        LEFT JOIN connexions cx ON cx.reunio_id = r.id
        WHERE r.grup_id = :grup_id
        GROUP BY r.id, r.font_id, r.data_reunio, r.departament,
                 r.activitat, r.tema_original, c.nom_canonical, c.titol
        ORDER BY r.data_reunio DESC
        LIMIT :limit OFFSET :offset
    """), {"grup_id": grup_id, "limit": limit, "offset": offset}).fetchall()

    total = db.execute(text(
        "SELECT COUNT(*) FROM reunions WHERE grup_id = :id"
    ), {"id": grup_id}).scalar()

    return {
        "total": total,
        "page": page,
        "items": [dict(r._mapping) for r in rows],
    }


@router.get("/{grup_id}/connexions")
def connexions_grup(
    grup_id: int,
    score_min: float = Query(50.0, ge=0, le=100),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db_fastapi),
):
    rows = db.execute(text("""
        SELECT
            cx.id, cx.connexio_score, cx.tipus_decisio,
            cx.dies_entre_reunio_decisio, cx.similitud_semantica,
            cx.similitud_departament, cx.explicacio_ca,
            r.data_reunio, r.tema_original,
            g.nom AS nom_grup,
            c.nom_canonical AS nom_carrec,
            -- Decisió
            nd.titol AS titol_decisio,
            nd.data_publicacio AS data_decisio,
            nd.url_dogc
        FROM connexions cx
        JOIN reunions r       ON r.id  = cx.reunio_id
        JOIN grups g          ON g.id  = r.grup_id
        LEFT JOIN carrecs c   ON c.id  = r.carrec_id
        LEFT JOIN normativa_dogc nd ON nd.id = cx.decisio_normativa_id
        WHERE r.grup_id = :grup_id
          AND cx.connexio_score >= :score_min
        ORDER BY cx.connexio_score DESC
        LIMIT :limit
    """), {"grup_id": grup_id, "score_min": score_min, "limit": limit}).fetchall()

    return {"items": [dict(r._mapping) for r in rows]}


@router.get("/{grup_id}/score")
def score_grup(grup_id: int, db=Depends(get_db_fastapi)):
    row = db.execute(text("""
        SELECT * FROM lobby_scores WHERE grup_id = :id
    """), {"id": grup_id}).fetchone()

    if not row:
        raise HTTPException(404, "Score no calculat per aquest grup")
    return dict(row._mapping)
