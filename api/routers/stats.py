"""Estadístiques globals i alertes."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from db.session import get_db_fastapi

router = APIRouter()


@router.get("/stats")
def estadistiques_globals(db=Depends(get_db_fastapi)):
    r = db.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM grups)         AS total_grups,
            (SELECT COUNT(*) FROM carrecs)        AS total_carrecs,
            (SELECT COUNT(*) FROM reunions)       AS total_reunions,
            (SELECT COUNT(*) FROM normativa_dogc) AS total_normativa_dogc,
            (SELECT COUNT(*) FROM connexions)     AS total_connexions,
            (SELECT COUNT(*) FROM connexions
             WHERE connexio_score >= 70)          AS connexions_alt_score,
            (SELECT MIN(data_reunio) FROM reunions WHERE data_reunio > '2000-01-01') AS primera_reunio,
            (SELECT MAX(data_reunio) FROM reunions) AS ultima_reunio
    """)).fetchone()
    return dict(r._mapping) if r else {}


@router.get("/alertes")
def alertes_recents(
    limit: int = 20,
    db=Depends(get_db_fastapi),
):
    rows = db.execute(text("""
        SELECT
            a.id, a.tipus, a.descripcio, a.creat_at,
            g.nom AS nom_grup
        FROM alertes a
        LEFT JOIN grups g ON g.id = a.grup_id
        ORDER BY a.creat_at DESC
        LIMIT :limit
    """), {"limit": limit}).fetchall()

    return {"items": [dict(r._mapping) for r in rows]}
