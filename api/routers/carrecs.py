"""Endpoints d'alts càrrecs."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from db.session import get_db_fastapi

router = APIRouter()


@router.get("")
def llista_carrecs(
    q: Optional[str] = Query(None),
    departament: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db_fastapi),
):
    offset = (page - 1) * limit
    filters, params = [], {"limit": limit, "offset": offset}

    if q:
        filters.append("c.nom_canonical ILIKE :q")
        params["q"] = f"%{q}%"
    if departament:
        filters.append("c.departament_codi = :dept")
        params["dept"] = departament.upper()

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = db.execute(text(f"""
        SELECT
            c.id, c.nom_canonical, c.titol, c.departament,
            c.tipologia,
            COUNT(r.id)          AS total_reunions,
            COUNT(DISTINCT r.grup_id) AS lobbies_contactats,
            MAX(r.data_reunio)   AS ultima_reunio
        FROM carrecs c
        LEFT JOIN reunions r ON r.carrec_id = c.id
        {where}
        GROUP BY c.id, c.nom_canonical, c.titol, c.departament, c.tipologia
        ORDER BY total_reunions DESC
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    total = db.execute(text(f"""
        SELECT COUNT(DISTINCT c.id) FROM carrecs c {where}
    """), {k: v for k, v in params.items() if k not in ("limit", "offset")}).scalar()

    return {"total": total, "page": page, "items": [dict(r._mapping) for r in rows]}


@router.get("/{carrec_id}")
def fitxa_carrec(carrec_id: int, db=Depends(get_db_fastapi)):
    row = db.execute(text("""
        SELECT
            c.id, c.nom_canonical, c.nom_original, c.titol,
            c.departament, c.departament_codi, c.tipologia,
            COUNT(r.id)               AS total_reunions,
            COUNT(DISTINCT r.grup_id) AS lobbies_contactats,
            MIN(r.data_reunio)        AS primera_reunio,
            MAX(r.data_reunio)        AS ultima_reunio
        FROM carrecs c
        LEFT JOIN reunions r ON r.carrec_id = c.id
        WHERE c.id = :id
        GROUP BY c.id, c.nom_canonical, c.nom_original, c.titol,
                 c.departament, c.departament_codi, c.tipologia
    """), {"id": carrec_id}).fetchone()

    if not row:
        raise HTTPException(404, "Càrrec no trobat")
    return dict(row._mapping)


@router.get("/{carrec_id}/reunions")
def reunions_carrec(
    carrec_id: int,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_db_fastapi),
):
    offset = (page - 1) * limit
    rows = db.execute(text("""
        SELECT
            r.id, r.font_id, r.data_reunio, r.departament,
            r.activitat, r.tema_original,
            g.nom AS nom_grup, g.id AS grup_id,
            COUNT(cx.id) AS num_connexions
        FROM reunions r
        LEFT JOIN grups g       ON g.id  = r.grup_id
        LEFT JOIN connexions cx ON cx.reunio_id = r.id
        WHERE r.carrec_id = :carrec_id
        GROUP BY r.id, r.font_id, r.data_reunio, r.departament,
                 r.activitat, r.tema_original, g.nom, g.id
        ORDER BY r.data_reunio DESC
        LIMIT :limit OFFSET :offset
    """), {"carrec_id": carrec_id, "limit": limit, "offset": offset}).fetchall()

    total = db.execute(text(
        "SELECT COUNT(*) FROM reunions WHERE carrec_id = :id"
    ), {"id": carrec_id}).scalar()

    return {"total": total, "page": page, "items": [dict(r._mapping) for r in rows]}
