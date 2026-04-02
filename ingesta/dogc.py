"""
Ingesta de la normativa del DOGC (dataset n6hn-rmy7).

CAMPS CONFIRMATS (crida real al JSON, 2026-04-01):
  any, n_mero_de_control, rang_de_norma, t_tol_de_la_norma,
  t_tol_de_la_norma_es, data_del_document, diari_oficial,
  n_mero_de_diari, data_de_publicaci_del_diari, vig_ncia_de_la_norma,
  format_html (objecte amb clau "url"), format_pdf (objecte amb clau "url")

NOTA: NO hi ha camp `departament` al dataset.
  El departament s'infereix del títol via expressió regular.
  Exemples reals:
    "Decret 13/2026, de 27 de gener, de reestructuració del Departament de Cultura"
    "Decret 12/2026, de 27 de gener, pel qual es modifica..."

COBERTURA: Lleis, Decrets i Ordres del DOGC des del 1977.
  Filtrem per any >= 2014 (suficient per cobrir totes les agendes existents).
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

DATASET_DOGC = "n6hn-rmy7"
ANY_DESDE = 2014

# Patrons per inferir el departament del títol de la norma
_DEPT_PATTERNS = [
    (r"Departament\s+d[e']?\s+([^,\.]+)", 1),
    (r"Departament\s+([^,\.]+)",           1),
    (r"departament\s+d[e']?\s+([^,\.]+)",  1),
]

# Prefixos de 2-3 lletres que apareixen a les Ordres/Resolucions del DOGC
# Format: "Ordre ARP/50/2026" → prefix "ARP" → nom del departament
_PREFIX_DEPT: dict[str, str] = {
    "ARP": "Agricultura, Ramaderia i Pesca",
    "ACC": "Acció Climàtica, Alimentació i Agenda Rural",
    "SLT": "Salut",
    "EMT": "Empresa i Treball",
    "EMC": "Empresa i Coneixement",
    "TSF": "Treball, Afers Socials i Famílies",
    "CLT": "Cultura",
    "EDU": "Educació",
    "EDF": "Educació i Formació Professional",
    "TER": "Territori",
    "TMT": "Territori, Mobilitat i Transports",
    "INT": "Interior",
    "IRP": "Interior i Relacions amb el Parlament",
    "JUS": "Justícia",
    "DSI": "Drets Socials i Inclusió",
    "DEM": "Drets i Memòria Democràtica",
    "PRE": "Presidència",
    "VPD": "Vicepresidència i Polítiques Digitals",
    "ECF": "Economia i Finances",
    "VEH": "Vicepresidència, Economia i Hisenda",
    "AEU": "Acció Exterior i Unió Europea",
    "HAC": "Hisenda",
    "SOC": "Afers Socials",
    "BEF": "Benestar Social i Família",
    "CIU": "Ciutadania",
    "GRI": "Governació i Relacions Institucionals",
    "ENS": "Ensenyament",
    "MED": "Medi Ambient",
    # Prefixos addicionals detectats a la BD
    "EMO": "Empresa i Ocupació",
    "ECO": "Economia i Coneixement",
    "AAM": "Agricultura, Alimentació i Acció Rural",
    "TES": "Territori i Sostenibilitat",
    "BSF": "Benestar Social i Família",
    "GAH": "Governació, Administracions Públiques i Habitatge",
    "PDA": "Polítiques Digitals i Administració Pública",
    "IFE": "Interior, Relacions Institucionals i Participació",
    "DSO": "Drets Socials",
    "EXT": "Acció Exterior",
    "REU": "Recerca i Universitats",
    "ESP": "Ensenyament",
    "XGO": "Governació",
    "APM": "Agricultura, Pesca i Medi Natural",
    "ISP": "Interior, Seguretat Pública",
    "UEX": "Acció Exterior i Unió Europea",
    "EXI": "Empresa i Innovació",
    "AEC": "Acció Exterior i Cooperació",
    "HFP": "Hisenda i Finances Públiques",
    "PRA": "Presidència",
}


def ingestar_normativa_dogc(db, any_desde: int = ANY_DESDE) -> dict:
    """
    Descarrega la normativa del DOGC des de l'any indicat.
    """
    from ingesta.socrata import fetch_dataset
    from normalitzacio.departaments import mapejar_departament

    stats = {"processats": 0, "nous": 0, "errors": 0}

    where = f"data_de_publicaci_del_diari >= '{any_desde}-01-01T00:00:00.000'"

    for row in fetch_dataset(
        DATASET_DOGC,
        where_clause=where,
        order_by="data_de_publicaci_del_diari DESC",
    ):
        try:
            d = _mapejar_fila_dogc(row)

            if not d["titol"] or not d["data_publicacio"]:
                continue

            is_nou = _upsert_normativa(db, d)
            if is_nou:
                stats["nous"] += 1
            stats["processats"] += 1

            if stats["processats"] % 1000 == 0:
                db.commit()
                logger.info(f"[dogc] {stats['processats']} normes ({stats['nous']} noves)")

        except Exception as e:
            logger.error(f"Error fila DOGC: {e}", exc_info=True)
            stats["errors"] += 1
            db.rollback()

    db.commit()
    logger.info(f"Ingesta DOGC completada: {stats}")
    return stats


def _mapejar_fila_dogc(row: dict) -> dict:
    """Mapeja els camps JSON reals de n6hn-rmy7."""
    titol = (row.get("t_tol_de_la_norma") or "").strip()
    data_pub = _parse_date(row.get("data_de_publicaci_del_diari") or "")
    tipus = (row.get("rang_de_norma") or "").strip()

    # URL: el camp format_html és un objecte {"url": "https://..."}
    url_html = ""
    format_html = row.get("format_html")
    if isinstance(format_html, dict):
        url_html = format_html.get("url", "")
    elif isinstance(format_html, str):
        url_html = format_html

    dept_raw = _inferir_departament_del_titol(titol)

    return {
        "font_id":        (row.get("n_mero_de_control") or "").strip(),
        "titol":          titol,
        "tipus_norma":    tipus,
        "departament":    dept_raw,
        "data_publicacio": data_pub,
        "num_dogc":       (row.get("n_mero_de_diari") or "").strip(),
        "url_dogc":       url_html,
        "vigencia":       (row.get("vig_ncia_de_la_norma") or "").strip(),
        "resum":          _construir_resum(row, titol, dept_raw),
    }


def _inferir_departament_del_titol(titol: str) -> str:
    """
    Extreu el nom del departament del títol de la norma.

    Estratègia dual:
    1. Cerca "Departament de X" al text de la norma.
    2. Si no, extreu el prefix de 2-4 lletres (ex. ARP, SLT, CLT) de
       "Ordre ARP/50/2026" i el mapeja a un nom de departament.

    Exemples:
      "Decret 13/2026, de reestructuració del Departament de Cultura"
      → "Departament de Cultura"

      "Ordre SLT/23/2026, de la consellera de Salut"
      → "Salut"
    """
    # Estratègia 1: "Departament de X" al cos del títol
    for pattern, group in _DEPT_PATTERNS:
        m = re.search(pattern, titol)
        if m:
            dept = m.group(group).strip().rstrip(".")
            if len(dept) <= 60:
                return dept

    # Estratègia 2: prefix codi al principi (ex. "Ordre ARP/50/2026")
    m = re.search(r"\b([A-Z]{2,4})/\d+/\d{4}", titol)
    if m:
        prefix = m.group(1)
        if prefix in _PREFIX_DEPT:
            return _PREFIX_DEPT[prefix]

    return ""


def _construir_resum(row: dict, titol: str, dept: str) -> str:
    """
    Construeix el text de resum per a l'embedding.
    Combina títol, tipus i departament inferit.
    """
    parts = [titol]
    tipus = (row.get("rang_de_norma") or "").strip()
    if tipus:
        parts.append(f"Tipus: {tipus}")
    if dept:
        parts.append(f"Departament: {dept}")
    return " | ".join(parts)[:2000]


def _upsert_normativa(db, d: dict) -> bool:
    """Crea o actualitza una norma DOGC. Retorna True si és nova."""
    from db.models import NormativaDogc
    from normalitzacio.departaments import mapejar_departament

    font_id = d["font_id"]
    if not font_id:
        return False

    existing = db.query(NormativaDogc).filter_by(font_id=font_id).first()
    # Estratègia de resolució de departament (2 nivells):
    # 1. Nom explícit extret del títol/prefix → mapejar_departament
    # 2. Keyword fallback sobre el títol complet (Lleis, Decrets sense prefix)
    #    → guarda el patró matchejat a dept_nom perquè quedi traçabilitat
    # Si res matcheja → GOVERN (acte transversal del Consell Executiu)
    from normalitzacio.departaments import mapejar_departament_amb_patro
    if d["departament"]:
        dept_codi = mapejar_departament(d["departament"])
        dept_nom = d["departament"]
    else:
        dept_codi, patro = mapejar_departament_amb_patro(d["titol"])
        dept_nom = f"[{patro}]" if patro else ""

    if existing:
        existing.resum = d["resum"]
        existing.departament = dept_nom
        existing.departament_codi = dept_codi
        return False

    norma = NormativaDogc(
        font_id=font_id,
        titol=d["titol"],
        tipus_norma=d["tipus_norma"],
        departament=dept_nom,
        departament_codi=dept_codi,
        data_publicacio=d["data_publicacio"],
        num_dogc=d["num_dogc"],
        url_dogc=d["url_dogc"],
        resum=d["resum"],
    )
    db.add(norma)
    return True


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None
