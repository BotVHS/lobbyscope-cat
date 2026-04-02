"""
Ingesta de contractes públics (hb6v-jcbf).

Dataset: "Contractació pública a Catalunya: inscripcions al Registre públic de contractes"
Conté els contractes adjudicats per la Generalitat i el seu sector públic.

CAMPS USATS:
  codi_expedient + numero_lot → font_id (clau composta)
  adjudicatari                → nom_adjudicatari + matching a grups
  import_adjudicacio          → import_euros
  data_adjudicacio            → data_adjudicacio
  descripcio_expedient        → objecte_contracte (finalitat per embedding)
  contracte                   → objecte_contracte (alternatiu)
  agrupacio_organisme         → departament (concessor)
  organisme_contractant       → departament_detall
  tipus_contracte             → metadada

ESTRATÈGIA DE MATCHING:
  Matching per nom normalitzat de l'adjudicatari (no hi ha CIF al dataset).
  Igual que subvencions: índex canonical + tokens significatius.

COBERTURA: des de 2014.
"""

import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

DATASET_CONTRACTES = "hb6v-jcbf"
ANY_DESDE = 2014


def ingestar_contractes(db) -> dict:
    """
    Descarrega els contractes públics i els vincula als grups d'interès.
    Pagina per any per evitar timeouts. ON CONFLICT DO NOTHING: idempotent.
    """
    from ingesta.socrata import fetch_dataset
    from db.models import Grup, ContracteLobby

    stats = {"processats": 0, "vinculats": 0, "errors": 0}

    logger.info("[contractes] Construint índex de grups...")
    grups = db.query(Grup).all()
    index_canonical: dict[str, Grup] = {g.nom_canonical: g for g in grups if g.nom_canonical}
    index_tokens: dict[str, list[Grup]] = {}
    for g in grups:
        tokens = _tokens_significatius(g.nom_canonical or "")
        if tokens:
            clau = " ".join(sorted(tokens[:3]))
            index_tokens.setdefault(clau, []).append(g)
    logger.info(f"[contractes] {len(grups)} grups indexats")

    any_actual = datetime.now().year
    BATCH_SIZE = 500
    # A partir de 2021, el dataset té >600K registres/any → paginació per mes
    ANY_MENSUAL_DESDE = 2021

    def _periodes(any_inici, any_fi):
        """Genera (data_inici, data_fi) per any o per mes si any >= ANY_MENSUAL_DESDE."""
        for a in range(any_inici, any_fi + 1):
            if a < ANY_MENSUAL_DESDE:
                yield f"{a}-01-01T00:00:00.000", f"{a+1}-01-01T00:00:00.000"
            else:
                for m in range(1, 13):
                    mes_seg = m + 1 if m < 12 else 1
                    any_seg = a if m < 12 else a + 1
                    if date(a, m, 1) > date(any_actual, datetime.now().month, 1):
                        return
                    yield f"{a}-{m:02d}-01T00:00:00.000", f"{any_seg}-{mes_seg:02d}-01T00:00:00.000"

    for periode_inici, periode_fi in _periodes(ANY_DESDE, any_actual):
        any_proc = int(periode_inici[:4])
        where = (
            f"data_adjudicacio >= '{periode_inici}' "
            f"AND data_adjudicacio < '{periode_fi}'"
        )
        batch: list = []
        any_proc_count = 0

        try:
            for row in fetch_dataset(DATASET_CONTRACTES, where_clause=where,
                                      page_size=2000, read_timeout=60):
                stats["processats"] += 1
                any_proc_count += 1

                d = _mapejar_fila(row)
                if not d["nom_adjudicatari"] or not d["data_adjudicacio"] or not d["font_id"]:
                    continue
                if d["lot_desert"]:
                    continue

                grup = _trobar_grup(d["nom_adjudicatari"], index_canonical, index_tokens)
                if not grup:
                    continue

                from normalitzacio.departaments import mapejar_departament
                dept_codi = mapejar_departament(d["departament"]) if d["departament"] else None
                batch.append(dict(
                    font_id=d["font_id"],
                    grup_id=grup.id,
                    nom_adjudicatari=d["nom_adjudicatari"],
                    import_euros=d["import_euros"],
                    departament=d["departament"],
                    departament_codi=dept_codi,
                    data_adjudicacio=d["data_adjudicacio"],
                    objecte_contracte=d["objecte_contracte"],
                ))
                stats["vinculats"] += 1

                if len(batch) >= BATCH_SIZE:
                    _commit_batch(db, ContracteLobby, batch, stats)
                    batch = []

        except Exception as e:
            logger.warning(f"[contractes] Error xarxa {periode_inici[:7]}: {e}. Continuant.")
            stats["errors"] += 1

        if batch:
            _commit_batch(db, ContracteLobby, batch, stats)

        logger.info(
            f"[contractes] {periode_inici[:7]}: {any_proc_count:,} processats — "
            f"total vinculats: {stats['vinculats']:,}"
        )

    logger.info(f"Ingesta contractes completada: {stats}")
    return stats


def _mapejar_fila(row: dict) -> dict:
    """Mapeja els camps JSON de hb6v-jcbf al nostre esquema."""
    codi = (row.get("codi_expedient") or "").strip()
    lot = (row.get("numero_lot") or "1").strip()
    font_id = f"{codi}_{lot}" if codi else ""

    # Objecte del contracte: prendre el camp més descriptiu
    objecte = (row.get("descripcio_expedient") or row.get("contracte") or "").strip()
    # Enriquir amb tipus i departament per a millor embedding
    tipus = (row.get("tipus_contracte") or "").strip()
    dept_org = (row.get("organisme_contractant") or "").strip()
    if tipus:
        objecte = f"{objecte} | {tipus}"
    if dept_org:
        objecte = f"{objecte} | {dept_org}"
    objecte = objecte[:2000]

    dept = (row.get("agrupacio_organisme") or "").strip()

    import_raw = row.get("import_adjudicacio") or "0"
    try:
        import_euros = float(str(import_raw).replace(",", "."))
    except (ValueError, TypeError):
        import_euros = 0.0

    lot_desert = (row.get("lot_desert") or "").strip().lower() in ("sí", "si", "yes", "true", "1")

    return {
        "font_id":          font_id,
        "nom_adjudicatari": (row.get("adjudicatari") or "").strip(),
        "import_euros":     import_euros,
        "departament":      dept,
        "data_adjudicacio": _parse_date(row.get("data_adjudicacio") or ""),
        "objecte_contracte": objecte,
        "lot_desert":       lot_desert,
    }


def _trobar_grup(nom_adjudicatari: str, index_canonical: dict, index_tokens: dict):
    """Intenta vincular un adjudicatari a un grup existent."""
    from normalitzacio.noms import normalitzar_nom_empresa

    canonical = normalitzar_nom_empresa(nom_adjudicatari)
    if not canonical:
        return None

    # 1. Match exacte
    if canonical in index_canonical:
        return index_canonical[canonical]

    # 2. Match per tokens (≥3 tokens ordenats)
    tokens = _tokens_significatius(canonical)
    if len(tokens) >= 2:
        clau = " ".join(sorted(tokens[:3]))
        candidats = index_tokens.get(clau, [])
        if len(candidats) == 1:
            return candidats[0]
        if candidats:
            for g in candidats:
                if g.nom_canonical and canonical in g.nom_canonical:
                    return g

    return None


def _tokens_significatius(canonical: str) -> list[str]:
    """Extreu tokens de ≥4 caràcters (paraules clau) d'un nom canonical."""
    stop = {"dels", "les", "per", "que", "amb", "una", "uns"}
    return [t for t in canonical.split() if len(t) >= 4 and t not in stop]


def _commit_batch(db, model, batch: list, stats: dict) -> None:
    """INSERT batch amb ON CONFLICT DO NOTHING."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    try:
        stmt = pg_insert(model).values(batch).on_conflict_do_nothing(
            index_elements=["font_id"]
        )
        db.execute(stmt)
        db.commit()
    except Exception as e:
        logger.error(f"Error commit batch ({len(batch)} registres): {e}")
        db.rollback()
        stats["errors"] += len(batch)


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None
