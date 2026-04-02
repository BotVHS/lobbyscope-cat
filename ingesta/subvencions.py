"""
Ingesta de subvencions i ajuts del RAISC (s9xt-n979).

RAISC = Registre d'Ajuts i Incentius a les Subvencions de Catalunya.
Conté totes les concessions de subvencions atorgades per la Generalitat.

CAMPS USATS:
  clau                              → font_id (clau primària del dataset)
  cif_beneficiari                   → cif_beneficiari (filtre: != "Benef. no publicable")
  ra_social_del_beneficiari         → nom_beneficiari + matching a grups
  t_tol_convocat_ria_catal          → part de la finalitat
  subfinalitat                      → part de la finalitat
  finalitat_rais                    → part de la finalitat
  objecte_de_la_convocat_ria        → part de la finalitat
  data_concessi                     → data_concessio
  import_subvenci_pr_stec_ajut      → import_euros
  departament_o_entitat_local_d_... → departament (concessor)

ESTRATÈGIA DE MATCHING:
  1. Normalitzar `ra_social_del_beneficiari` → clau canonical
  2. Buscar coincidència a l'índex en memòria de grups.nom_canonical
  3. Només desar si hi ha grup coincident (grup_id NOT NULL)
  4. Si CIF disponible i no estava al grup → guardar-lo

COBERTURA: des de 2014 (alineat amb les agendes i el DOGC).
"""

import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

DATASET_RAISC = "s9xt-n979"
ANY_DESDE = 2014


def ingestar_subvencions(db) -> dict:
    """
    Descarrega les subvencions del RAISC i les vincula als grups d'interès.
    Pagina per any (ANY_DESDE..any_actual) per evitar timeouts amb offsets grans.
    Usa ON CONFLICT DO NOTHING: és idempotent i es pot reprendre.
    """
    from ingesta.socrata import fetch_dataset
    from db.models import Grup, SubvencioLobby
    from sqlalchemy import text

    stats = {"processades": 0, "vinculades": 0, "errors": 0}

    # Index en memòria: nom_canonical → grup
    logger.info("[subvencions] Construint índex de grups...")
    grups = db.query(Grup).all()
    index_canonical: dict[str, Grup] = {g.nom_canonical: g for g in grups if g.nom_canonical}
    index_tokens: dict[str, list[Grup]] = {}
    for g in grups:
        tokens = _tokens_significatius(g.nom_canonical or "")
        if tokens:
            clau = " ".join(sorted(tokens[:3]))
            index_tokens.setdefault(clau, []).append(g)
    logger.info(f"[subvencions] {len(grups)} grups indexats")

    any_actual = datetime.now().year
    BATCH_SIZE = 500

    # Paginació per any: cada any té ~75K registres → cap problema de timeout
    for any_proc in range(ANY_DESDE, any_actual + 1):
        where = (
            f"data_concessi >= '{any_proc}-01-01T00:00:00.000' "
            f"AND data_concessi < '{any_proc + 1}-01-01T00:00:00.000' "
            f"AND tipus_de_beneficiaris_codi != 'FSA'"
        )
        batch: list = []
        any_proc_count = 0

        for row in fetch_dataset(DATASET_RAISC, where_clause=where,
                                  page_size=2000, read_timeout=60):
            stats["processades"] += 1
            any_proc_count += 1

            d = _mapejar_fila(row)
            if not d["nom_beneficiari"] or not d["data_concessio"] or not d["font_id"]:
                continue

            grup = _trobar_grup(d["nom_beneficiari"], index_canonical, index_tokens)
            if not grup:
                continue

            if d["cif_beneficiari"] and not grup.cif:
                grup.cif = d["cif_beneficiari"]

            from normalitzacio.departaments import mapejar_departament
            dept_codi = mapejar_departament(d["departament"]) if d["departament"] else None
            batch.append(dict(
                font_id=d["font_id"],
                grup_id=grup.id,
                cif_beneficiari=d["cif_beneficiari"] or None,
                nom_beneficiari=d["nom_beneficiari"],
                import_euros=d["import_euros"],
                departament=d["departament"],
                departament_codi=dept_codi,
                data_concessio=d["data_concessio"],
                finalitat=d["finalitat"],
            ))
            stats["vinculades"] += 1

            if len(batch) >= BATCH_SIZE:
                _commit_batch(db, SubvencioLobby, batch, stats)
                batch = []

        if batch:
            _commit_batch(db, SubvencioLobby, batch, stats)

        logger.info(
            f"[subvencions] Any {any_proc}: {any_proc_count:,} processades — "
            f"total vinculades: {stats['vinculades']:,}"
        )

    logger.info(f"Ingesta subvencions completada: {stats}")
    return stats


def _mapejar_fila(row: dict) -> dict:
    """Mapeja els camps JSON de s9xt-n979 al nostre esquema."""
    # Construir finalitat combinant varis camps descriptius
    parts_finalitat = []
    for camp in ["t_tol_convocat_ria_catal", "objecte_de_la_convocat_ria",
                  "subfinalitat", "finalitat_rais"]:
        v = (row.get(camp) or "").strip()
        if v and v not in parts_finalitat:
            parts_finalitat.append(v)
    finalitat = " | ".join(parts_finalitat)[:2000]

    # Departament concessor (el que atorga la subvenció)
    dept = (
        row.get("departament_o_entitat_local_d_adscripci_")
        or row.get("entitat_oo_aa_o_departament_1")
        or ""
    ).strip()

    import_raw = row.get("import_subvenci_pr_stec_ajut") or "0"
    try:
        import_euros = float(str(import_raw).replace(",", "."))
    except (ValueError, TypeError):
        import_euros = 0.0

    return {
        "font_id":        (row.get("clau") or "").strip(),
        "cif_beneficiari": (row.get("cif_beneficiari") or "").strip(),
        "nom_beneficiari": (row.get("ra_social_del_beneficiari") or "").strip(),
        "data_concessio": _parse_date(row.get("data_concessi") or ""),
        "import_euros":   import_euros,
        "departament":    dept,
        "finalitat":      finalitat,
    }


def _commit_batch(db, model, batch: list, stats: dict) -> None:
    """INSERT batch amb ON CONFLICT DO NOTHING (evita duplicats de paginació)."""
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


def _trobar_grup(nom_beneficiari: str, index_canonical: dict, index_tokens: dict):
    """
    Intenta vincular un nom de beneficiari a un grup existent.

    Prioritat:
    1. Match exacte per nom canonical
    2. Match parcial per tokens significatius (mínim 2 tokens coincidents)
    """
    from normalitzacio.noms import normalitzar_nom_empresa

    canonical = normalitzar_nom_empresa(nom_beneficiari)
    if not canonical:
        return None

    # 1. Match exacte
    if canonical in index_canonical:
        return index_canonical[canonical]

    # 2. Match per tokens (fins 3 tokens ordenats)
    tokens = _tokens_significatius(canonical)
    if len(tokens) >= 2:
        clau = " ".join(sorted(tokens[:3]))
        candidats = index_tokens.get(clau, [])
        if len(candidats) == 1:
            return candidats[0]
        # Si múltiples candidats, agafar el que té el canonical més semblant
        if candidats:
            for g in candidats:
                if g.nom_canonical and canonical in g.nom_canonical:
                    return g

    return None


def _tokens_significatius(canonical: str) -> list[str]:
    """Extreu tokens de ≥4 caràcters (paraules clau) d'un nom canonical."""
    stop = {"dels", "dels", "les", "per", "que", "amb", "una", "uns"}
    return [t for t in canonical.split() if len(t) >= 4 and t not in stop]



def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None
