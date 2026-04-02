"""
Ingesta del dataset hd8k-y28e: Agenda pública amb grups d'interès.

CAMPS CONFIRMATS (crida real al JSON, 2026-04-01):
  id, departament, unitat_org_nica, data, grup_d_inter_s,
  nom_registre_grup_inter_s, inscripci_al_rgi, n_mero_de_rgi,
  activitat, tema, nom_i_cognoms, c_rrec, tipologia

NOTA: `n_mero_de_rgi` és el número del Registre de Grups d'Interès
(clau de creuament amb gwpn-de62 via `num_inscripcio`).
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

DATASET_AGENDES = "hd8k-y28e"


def ingestar_agendes(db) -> dict:
    """
    Descarrega i carrega totes les reunions del dataset hd8k-y28e.
    Crea o actualitza carrecs i grups associats.
    """
    from ingesta.socrata import fetch_dataset
    from normalitzacio.noms import normalitzar_nom_persona
    from normalitzacio.departaments import mapejar_departament

    stats = {"processats": 0, "nous": 0, "actualitzats": 0, "errors": 0}

    for row in fetch_dataset(DATASET_AGENDES, order_by="data DESC"):
        try:
            d = _mapejar_fila(row)

            if not d.get("font_id") or not d.get("data_reunio"):
                continue

            # Temes molt curts no aporten semàntica útil per a embeddings
            if len(d.get("tema_original", "")) < 15:
                continue

            carrec = _get_or_create_carrec(db, d)
            grup = _get_or_create_grup(db, d)
            is_nou = _upsert_reunio(db, d, carrec, grup)

            if is_nou:
                stats["nous"] += 1
            else:
                stats["actualitzats"] += 1
            stats["processats"] += 1

            if stats["processats"] % 500 == 0:
                db.commit()
                logger.info(
                    f"[agendes] {stats['processats']} processats "
                    f"({stats['nous']} nous, {stats['actualitzats']} actualitzats)"
                )

        except Exception as e:
            logger.error(f"Error fila {row.get('id', '?')}: {e}", exc_info=True)
            stats["errors"] += 1
            db.rollback()

    db.commit()
    logger.info(f"Ingesta agendes completada: {stats}")
    return stats


def _mapejar_fila(row: dict) -> dict:
    """Mapeja els camps JSON reals al nostre esquema intern."""
    return {
        "font_id":           row.get("id", "").strip(),
        "departament":       row.get("departament", "").strip(),
        "unitat_organica":   row.get("unitat_org_nica", "").strip(),
        "data_reunio":       _parse_date(row.get("data", "")),
        "nom_grup_original": row.get("grup_d_inter_s", "").strip(),
        "nom_registre_grup": row.get("nom_registre_grup_inter_s", "").strip(),
        "situacio_inscripcio": row.get("inscripci_al_rgi", "").strip(),
        "codi_registre":     row.get("n_mero_de_rgi", "").strip(),
        "activitat":         row.get("activitat", "").strip(),
        "tema_original":     row.get("tema", "").strip(),
        "nom_carrec":        row.get("nom_i_cognoms", "").strip(),
        "titol_carrec":      row.get("c_rrec", "").strip(),
        "tipologia":         row.get("tipologia", "").strip(),
    }


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    logger.warning(f"Data no parsejable: {s!r}")
    return None


def _get_or_create_carrec(db, d: dict):
    from normalitzacio.noms import normalitzar_nom_persona
    from normalitzacio.departaments import mapejar_departament
    from db.models import Carrec

    nom_original = d.get("nom_carrec", "")
    if not nom_original.strip():
        return None

    nom_canonical = normalitzar_nom_persona(nom_original)
    dept_codi = mapejar_departament(d.get("departament", ""))

    carrec = db.query(Carrec).filter_by(
        nom_canonical=nom_canonical,
        departament_codi=dept_codi,
    ).first()

    if not carrec:
        carrec = Carrec(
            nom_canonical=nom_canonical,
            nom_original=nom_original,
            nom_tokens=nom_canonical.lower().split(),
            titol=d.get("titol_carrec", ""),
            departament=d.get("departament", ""),
            departament_codi=dept_codi,
            tipologia=d.get("tipologia", ""),
        )
        db.add(carrec)
        db.flush()
    else:
        # Actualitzar tipologia si ha canviat (ascens, cessament...)
        if d.get("tipologia"):
            carrec.tipologia = d["tipologia"]

    return carrec


def _get_or_create_grup(db, d: dict):
    from db.models import Grup
    from normalitzacio.noms import normalitzar_nom_empresa

    nom_original = d.get("nom_grup_original", "")
    codi_registre = d.get("codi_registre", "")

    if not nom_original.strip() and not codi_registre.strip():
        return None

    # Prioritat: codi_registre (clau oficial del Registre de Grups d'Interès)
    grup = None
    if codi_registre.strip():
        grup = db.query(Grup).filter_by(codi_registre=codi_registre.strip()).first()

    # Fallback: nom canonical
    if not grup and nom_original.strip():
        nom_canonical = normalitzar_nom_empresa(nom_original)
        if nom_canonical:
            grup = db.query(Grup).filter_by(nom_canonical=nom_canonical).first()

    if not grup:
        nom_canonical = normalitzar_nom_empresa(nom_original) or nom_original.lower()
        grup = Grup(
            codi_registre=codi_registre.strip() or None,
            nom=nom_original,
            nom_canonical=nom_canonical,
            situacio_inscripcio=d.get("situacio_inscripcio", ""),
        )
        db.add(grup)
        db.flush()
    else:
        if codi_registre.strip() and not grup.codi_registre:
            grup.codi_registre = codi_registre.strip()
        if d.get("situacio_inscripcio"):
            grup.situacio_inscripcio = d["situacio_inscripcio"]

    return grup


def _upsert_reunio(db, d: dict, carrec, grup) -> bool:
    """Crea o actualitza una reunió. Retorna True si és nova."""
    from db.models import Reunio
    from normalitzacio.departaments import mapejar_departament

    existing = db.query(Reunio).filter_by(font_id=d["font_id"]).first()
    dept_codi = mapejar_departament(d.get("departament", ""))
    tema_net = _netejar_tema(d.get("tema_original", ""))

    if existing:
        existing.tema_normalitzat = tema_net
        existing.departament_codi = dept_codi
        return False

    reunio = Reunio(
        font_id=d["font_id"],
        carrec_id=carrec.id if carrec else None,
        grup_id=grup.id if grup else None,
        data_reunio=d["data_reunio"],
        departament=d.get("departament", ""),
        departament_codi=dept_codi,
        unitat_organica=d.get("unitat_organica", ""),
        activitat=d.get("activitat", ""),
        tema_original=d.get("tema_original", ""),
        tema_normalitzat=tema_net,
        nom_grup_original=d.get("nom_grup_original", ""),
        nom_registre_grup=d.get("nom_registre_grup", ""),
        situacio_inscripcio=d.get("situacio_inscripcio", ""),
    )
    db.add(reunio)
    return True


def _netejar_tema(tema: str) -> str:
    """Neteja el text del tema per a millor qualitat d'embedding."""
    if not tema:
        return ""
    tema = tema.strip().strip('"\'')
    prefixos = [
        r"^Visita institucional\.?\s*",
        r"^Presentació de l[\'']entitat\.?\s*",
        r"^Reunió de treball\.?\s*",
        r"^Acte de protocol\.?\s*",
    ]
    for prefix in prefixos:
        tema = re.sub(prefix, "", tema, flags=re.IGNORECASE).strip()
    return tema[:1000]
