"""
Enriquiment dels grups amb les dades del Registre de Grups d'Interès (gwpn-de62).

CAMPS CONFIRMATS (crida real al JSON, 2026-04-01):
  nom, id_grup_interes, identificador, data_alta, tipus_grup,
  pagina_web, email, finalitat, categoria_registre, subcategoria_registre,
  ambits_interes (string separat per |), num_inscripcio (no sempre present),
  rao_social, nom_representant, ...

CLAU DE CREUAMENT:
  hd8k-y28e: n_mero_de_rgi  (p. ex. "449", "4859")
  gwpn-de62:  num_inscripcio (p. ex. "677") — NOMÉS en grups antics
              Els grups nous (des de ~2023) usen id_grup_interes com a clau.

ESTRATÈGIA:
  1. Primer intentar creuament per num_inscripcio ↔ codi_registre
  2. Si no, creuament per nom canonical (fuzzy)
  3. Guardar id_grup_interes per a futures referències
"""

import logging

logger = logging.getLogger(__name__)

DATASET_REGISTRE = "gwpn-de62"


def enriquir_grups(db) -> dict:
    """
    Per a cada grup ja existent a la BD, busca les seves dades
    ampliades al Registre de Grups d'Interès (gwpn-de62).
    A més, crea grups nous que apareguin al registre però no a les agendes.
    """
    from ingesta.socrata import fetch_dataset
    from db.models import Grup

    stats = {"actualitzats": 0, "nous_del_registre": 0, "errors": 0}

    # Índex per codi_registre (num_inscripcio dels antics)
    grups_per_codi: dict[str, Grup] = {
        g.codi_registre: g
        for g in db.query(Grup).filter(Grup.codi_registre.isnot(None)).all()
    }

    # Índex per nom canonical (fallback)
    grups_per_nom: dict[str, Grup] = {
        g.nom_canonical: g
        for g in db.query(Grup).all()
    }

    for row in fetch_dataset(DATASET_REGISTRE):
        try:
            d = _mapejar_fila_grup(row)

            if not d["nom"]:
                continue

            grup = _trobar_grup_existent(d, grups_per_codi, grups_per_nom)

            if grup:
                _actualitzar_grup(grup, d)
                stats["actualitzats"] += 1
            else:
                nou = _crear_grup_del_registre(db, d)
                if nou:
                    stats["nous_del_registre"] += 1
                    # Afegir al índex per a creuaments posteriors de la mateixa iteració
                    if nou.codi_registre:
                        grups_per_codi[nou.codi_registre] = nou
                    if nou.nom_canonical:
                        grups_per_nom[nou.nom_canonical] = nou

        except Exception as e:
            logger.error(f"Error enriquint grup '{row.get('nom', '?')}': {e}")
            stats["errors"] += 1
            db.rollback()

        total = stats["actualitzats"] + stats["nous_del_registre"]
        if total > 0 and total % 500 == 0:
            db.commit()
            logger.info(f"[grups] {total} processats ({stats['actualitzats']} act., {stats['nous_del_registre']} nous)")

    db.commit()
    logger.info(f"Enriquiment grups completat: {stats}")
    return stats


def _mapejar_fila_grup(row: dict) -> dict:
    """Mapeja els camps JSON reals de gwpn-de62."""
    # ambits_interes és un string separat per "|"
    ambits_raw = row.get("ambits_interes", "") or ""
    if ambits_raw:
        ambits = [a.strip() for a in ambits_raw.split("|") if a.strip()]
    else:
        ambits = []

    # num_inscripcio és el número del RGI (el que apareix a n_mero_de_rgi en agendes)
    num_inscripcio = str(row.get("num_inscripcio", "") or "").strip()

    return {
        "id_grup_interes":  row.get("id_grup_interes", "").strip(),
        "identificador":    row.get("identificador", "").strip(),
        "num_inscripcio":   num_inscripcio,
        "nom":              (row.get("nom") or "").strip(),
        "rao_social":       (row.get("rao_social") or "").strip(),
        "tipus_grup":       (row.get("tipus_grup") or "").strip(),
        "finalitat":        (row.get("finalitat") or "").strip(),
        "categoria":        (row.get("categoria_registre") or "").strip(),
        "subcategoria":     (row.get("subcategoria_registre") or "").strip(),
        "ambits_interes":   ambits,
        "pagina_web":       (row.get("pagina_web") or "").strip(),
    }


def _trobar_grup_existent(d: dict, per_codi: dict, per_nom: dict):
    """
    Intenta trobar un grup existent.
    Prioritat: num_inscripcio > nom canonical.
    """
    from normalitzacio.noms import normalitzar_nom_empresa

    # 1. Per num_inscripcio (clau oficial als datasets antics)
    if d["num_inscripcio"]:
        grup = per_codi.get(d["num_inscripcio"])
        if grup:
            return grup

    # 2. Per nom canonical
    nom_canonical = normalitzar_nom_empresa(d["nom"])
    if nom_canonical:
        return per_nom.get(nom_canonical)

    return None


def _actualitzar_grup(grup, d: dict) -> None:
    """Enriqueix un grup existent amb les dades del registre."""
    if d["finalitat"] and not grup.objectius:
        grup.objectius = d["finalitat"]
    if d["ambits_interes"]:
        grup.ambit_interes = d["ambits_interes"]
    if not grup.codi_registre and d["num_inscripcio"]:
        grup.codi_registre = d["num_inscripcio"]


def _crear_grup_del_registre(db, d: dict):
    """Crea un grup nou que existeix al registre però no a les agendes."""
    from db.models import Grup
    from normalitzacio.noms import normalitzar_nom_empresa

    nom = d["nom"] or d["rao_social"]
    if not nom:
        return None

    nom_canonical = normalitzar_nom_empresa(nom) or nom.lower().strip()

    # Verificar a la BD per si hi ha un concurrent creat al mateix batch
    if d["num_inscripcio"]:
        existent = db.query(Grup).filter(Grup.codi_registre == d["num_inscripcio"]).first()
        if existent:
            _actualitzar_grup(existent, d)
            return existent

    grup = Grup(
        codi_registre=d["num_inscripcio"] or None,
        nom=nom,
        nom_canonical=nom_canonical,
        situacio_inscripcio="Inscrit",
        ambit_interes=d["ambits_interes"] or [],
        objectius=d["finalitat"] or None,
    )
    db.add(grup)
    db.flush()
    return grup
