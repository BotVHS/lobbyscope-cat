"""
Mapeig de noms de departaments de la Generalitat de Catalunya
a codis canònics.

Robustesa:
  - Variacions d'escriptura ("Dept.", "Departament de", sense article)
  - Majúscules/minúscules
  - Accents
  - Noms antics (reorganitzacions de conselleries)

Ús:
  >>> from normalitzacio.departaments import mapejar_departament
  >>> mapejar_departament("Departament de Salut")
  'SALUT'
  >>> mapejar_departament("dept. de salut")
  'SALUT'
  >>> mapejar_departament("")
  'DESCONEGUT'
"""

import re
import unicodedata

from unidecode import unidecode


# Codi canònic → llista de patrons (minúscules, sense accents, sense "departament de")
# L'ordre és irrellevant; es fa matching per substring.
_MAPEIG: dict[str, list[str]] = {
    "SALUT": [
        "salut",
        "health",
        "sanitat",
        "sanidad",
    ],
    "EDUCACIO": [
        "educacio",
        "educacion",
        "ensenyament",
        "formacio professional",
        "ciencia",
        "ciencies",
    ],
    "TERRITORI": [
        "territori i medi ambient",
        "territori i sostenibilitat",
        "territori i habitatge",
        "politica territorial",
        "infraestructures",
        "infraestructuras",
        "obres publiques",
        "urbanisme",
        "territori",
        "territorio",
        "habitatge",
        "vivienda",
        "mobilitat",
        "movilidad",
    ],
    "EMPRESA": [
        "empresa i treball",
        "empresa i coneixement",
        "empresa",
        "treball",
        "trabajo",
        "industria",
        "ocupacio",
        "empleo",
        "comerc",
        "comercio",
        "turisme",
        "turismo",
        "firal",
        "fires",
    ],
    "INTERIOR": [
        "interior",
        "seguretat",
        "seguridad",
        "policia",
        "emergencies",
        "emergencias",
        "proteccio civil",
    ],
    "JUSTICIA": [
        "justicia",
        "drets i memoria",
        "qualitat democratica",
        "memoria democratica",
        "rehabilitacio",
    ],
    "IGUALTAT": [
        "igualtat i feminismes",
        "igualtat i feminisme",
        "drets socials i inclusio",
        "treball afers socials",
        "igualtat",
        "igualdad",
        "feminismes",
        "feminismos",
        "afers socials",
        "servicios sociales",
        "serveis socials",
        "familia",
        "infancia",
        "benestar social",
        "bienestar",
        "politiques socials",
        "inclusio social",
        "drets socials",
    ],
    "PRESIDENCIA": [
        "president de la generalitat",
        "vicepresidencia i politiques digitals",
        "vicepresidencia executiva",
        "presidencia",
        "vicepresidencia",
        "relacions institucionals",
        "relaciones institucionales",
        "govern obert",
        "govern",
        "gobierno",
        "accio exterior",
        "accio i unio europea",
        "unio europea",
        "secretaria general",
        "portaveu",
        "gabinet",
        "politiques digitals",
        "administracio publica",
        "digitalitzacio",
        "telecomunicacions",
    ],
    "ECONOMIA": [
        "economia i finances",
        "economia i hisenda",
        "economia",
        "finances",
        "hacienda",
        "hisenda",
        "pressupostos",
        "presupuestos",
        "tresoreria",
        "tributs",
    ],
    "CULTURA": [
        "cultura",
        "esports",
        "deportes",
        "patrimoni",
        "patrimonio",
        "llengua",
        "lengua",
        "politica linguistica",
        "audiovisual",
        "museus",
    ],
    "ACCIO_CLIMATICA": [
        "accio climatica",
        "medi ambient i habitatge",
        "medi ambient",
        "medio ambiente",
        "alimentacio",
        "alimentacion",
        "agricultura",
        "pesca",
        "ramaderia",
        "ganaderia",
        "sostenibilitat",
        "sostenibilidad",
        "transicio ecologica",
        "transicio energetica",
        "agenda rural",
        "energia",
    ],
    "RECERCA": [
        "recerca i universitats",
        "recerca",
        "investigacio",
        "investigacion",
        "innovacio",
        "innovacion",
        "universitats",
    ],
    # Òrgans independents i entitats transversals del Govern (no departaments)
    "GOVERN": [
        "autoritat catalana de la competencia",
        "institut catala internacional per la pau",
        "comissio de garantia del dret d acces",
        "agencia catalana de proteccio de dades",
        "consell de garanties estatutaries",
        "consell de l audiovisual de catalunya",
        "tribunal catala de contractes del sector public",
        "comissio juridica assessora",
        "sindic de greuges",
        "sindicatura de comptes",
        "consell assessor",
        "consell executiu",
        "govern de la generalitat",
        "generalitat de catalunya",
        "icip",
        "gaip",
    ],
}

# Pre-computar llista plana (codi, patro) ordenada per longitud DESC globalment.
# Sort global: patrons llargs (específics) es comproven SEMPRE abans que curts (genèrics),
# independentment del codi — evita que "treball"(7) guanyi a "afers socials"(13),
# o que "habitatge"(9) guanyi a "medi ambient i habitatge"(24), etc.
_MAPEIG_ORDENAT: list[tuple[str, str]] = sorted(
    [
        (codi, patro)
        for codi, patrons in _MAPEIG.items()
        for patro in patrons
    ],
    key=lambda x: len(x[1]),
    reverse=True,
)


def mapejar_departament(s: str) -> str:
    """
    Retorna el codi canònic del departament.

    - Si el text és buit → 'DESCONEGUT' (dada absent, no és un departament conegut)
    - Si hi ha text però no matcheja cap patró → 'GOVERN' (òrgan/acte transversal
      del Govern: comissions, òrgans independents, Lleis transversals, etc.)

    Args:
        s: Nom del departament tal com apareix al dataset.

    Returns:
        Codi canònic en majúscules (p. ex. 'SALUT', 'EDUCACIO', 'GOVERN').
    """
    if not s or not s.strip():
        return "DESCONEGUT"

    normalitzat = _normalitzar_per_matching(s)

    for codi, patro in _MAPEIG_ORDENAT:
        # Word-boundary: evita falsos positius com "cultura" dins "agricultura"
        if re.search(r"(?<![a-z])" + re.escape(patro) + r"(?![a-z])", normalitzat):
            return codi

    # Text present però sense departament específic → òrgan/acte transversal del Govern
    return "GOVERN"


def mapejar_departament_amb_patro(s: str) -> tuple[str, str]:
    """
    Com mapejar_departament, però retorna també el patró que ha fet match.

    Returns:
        (codi, patro_matchejat)
        Ex: ('TERRITORI', 'habitatge')  o  ('GOVERN', '')

    Útil per guardar quin keyword ha inferit el departament quan no hi ha
    un nom explícit al document (p. ex. fallback per keywords del títol DOGC).
    """
    if not s or not s.strip():
        return "DESCONEGUT", ""

    normalitzat = _normalitzar_per_matching(s)

    for codi, patro in _MAPEIG_ORDENAT:
        if re.search(r"(?<![a-z])" + re.escape(patro) + r"(?![a-z])", normalitzat):
            return codi, patro

    return "GOVERN", ""


def _normalitzar_per_matching(s: str) -> str:
    """
    Prepara un string per al matching: minúscules, sense accents,
    sense prefixos comuns ("departament de", "dept.", "conselleria de").
    """
    # Minúscules
    s = s.lower().strip()

    # Eliminar accents
    s = unidecode(s)

    # Eliminar prefixos comuns
    prefixos = [
        r"^departament\s+d[e']?\s+",
        r"^departament\s+",
        r"^dept\.\s+d[e']?\s+",
        r"^dept\.\s+",
        r"^conselleria\s+d[e']?\s+",
        r"^conselleria\s+",
        r"^secretaria\s+d[e']?\s+",
    ]
    for prefix in prefixos:
        s = re.sub(prefix, "", s)

    # Eliminar puntuació
    s = re.sub(r"[,\.\-;:\(\)]", " ", s)

    # Netejar espais múltiples
    s = re.sub(r"\s+", " ", s).strip()

    return s
