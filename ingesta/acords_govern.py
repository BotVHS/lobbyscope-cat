"""
Ingesta dels Acords del Govern (dataset ub8p-uqwj).

CAMPS CONFIRMATS (crida real al JSON):
  codi, departament, titol, any, datasessio,
  document1 (objecte amb clau "url"), tipus_document

COBERTURA: Decisions setmanals del Consell Executiu des de 2014.
  ~12.600 acords, ~1.200-1.400/any (2018+).
  Molt rellevants per lobbying: autoritzacions de subvencions,
  aprovació de plans sectorials, modificacions normatives d'urgència.

NOTA: No hi ha camp `departament_codi` al dataset.
  El departament s'infereix via mapejar_departament() igual que a dogc.py.
"""

import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

DATASET_ACORDS = "ub8p-uqwj"
ANY_DESDE = 2014


def ingestar_acords_govern(db, any_desde: int = ANY_DESDE) -> dict:
    """
    Descarrega els Acords del Govern des de l'any indicat.
    Idempotent: ON CONFLICT DO NOTHING sobre font_id (= codi).
    """
    from ingesta.socrata import fetch_dataset
    from normalitzacio.departaments import mapejar_departament
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from db.models import AcordGovern

    stats = {"processats": 0, "nous": 0, "errors": 0}

    where = f"datasessio >= '{any_desde}-01-01T00:00:00.000'"

    batch: list[dict] = []

    def _commit_batch(b: list[dict]) -> int:
        if not b:
            return 0
        stmt = pg_insert(AcordGovern).values(b).on_conflict_do_nothing(
            index_elements=["font_id"]
        )
        result = db.execute(stmt)
        db.commit()
        return result.rowcount or 0

    for row in fetch_dataset(
        DATASET_ACORDS,
        where_clause=where,
        order_by="datasessio DESC",
    ):
        try:
            d = _mapejar_fila(row)
            if not d:
                continue

            batch.append(d)
            stats["processats"] += 1

            if len(batch) >= 500:
                nous = _commit_batch(batch)
                stats["nous"] += nous
                batch.clear()
                logger.info(
                    f"[acords] {stats['processats']} processats "
                    f"({stats['nous']} nous)"
                )

        except Exception as e:
            logger.error(f"Error fila acord: {e}", exc_info=True)
            stats["errors"] += 1
            db.rollback()
            batch.clear()

    # Últim batch
    stats["nous"] += _commit_batch(batch)

    logger.info(f"Ingesta Acords del Govern completada: {stats}")
    return stats


def _mapejar_fila(row: dict) -> Optional[dict]:
    """Mapeja els camps JSON de ub8p-uqwj a un dict per inserir."""
    from normalitzacio.departaments import mapejar_departament

    font_id = (row.get("codi") or "").strip()
    titol = (row.get("titol") or "").strip()
    data_sessio = _parse_date(row.get("datasessio") or "")

    if not font_id or not titol or not data_sessio:
        return None

    dept_raw = (row.get("departament") or "").strip()
    dept_codi = mapejar_departament(dept_raw) if dept_raw else "GOVERN"

    # URL del primer document adjunt
    url = ""
    doc1 = row.get("document1")
    if isinstance(doc1, dict):
        url = doc1.get("url", "")
    elif isinstance(doc1, str):
        url = doc1

    # Resum per embedding: titol + departament
    resum_parts = [titol]
    if dept_raw:
        resum_parts.append(f"Departament: {dept_raw}")
    resum = " | ".join(resum_parts)[:2000]

    return {
        "font_id":        font_id,
        "titol":          titol,
        "departament":    dept_raw,
        "departament_codi": dept_codi,
        "data_sessio":    data_sessio,
        "url_document":   url,
        "resum":          resum,
    }


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None
