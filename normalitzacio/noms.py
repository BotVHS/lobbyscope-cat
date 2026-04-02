"""
Normalització de noms de persones i empreses per a lobbyscope.cat.

Funcions principals:
  - normalitzar_nom_persona(s) → Title Case amb partícules catalanes en minúscula
  - normalitzar_nom_empresa(s) → clau canonical lowercase sense accents ni formes jurídiques

Exemples:
  >>> normalitzar_nom_persona("AINA PLAZA TESÍAS")
  'Aina Plaza Tesías'
  >>> normalitzar_nom_persona("teresa jordà i roura")
  'Teresa Jordà i Roura'
  >>> normalitzar_nom_empresa("FUNDACIÓ HOSPITAL SANT JOAN DE DÉU, S.A.")
  'fundacio hospital sant joan de deu'
"""

import re
import unicodedata

from unidecode import unidecode


# Partícules catalanes que resten en minúscula al mig d'un nom
_PARTICULES_CA = frozenset({
    "i", "de", "del", "dels", "d", "de la", "de les", "de l",
    "l", "la", "les", "els", "en", "na",
})

# Formes jurídiques a eliminar del nom canonical d'empresa
_FORMES_JURIDIQUES = re.compile(
    r"\b(s\.?l\.?u?|s\.?a\.?u?|s\.?c\.?p\.?|s\.?c\.?o\.?o\.?p\.?|"
    r"s\.?l\.?l\.?|s\.?a\.?t\.?|a\.?i\.?e\.?|g\.?i\.?e\.?|"
    r"s\.?l\.?n\.?e\.?|fundaci[oó]|associaci[oó]|cooperativa|"
    r"consorci|patronat|institut|servei[s]?|grup|corporaci[oó])\b",
    re.IGNORECASE,
)

# Caràcters a eliminar del canonical d'empresa (puntuació innecessària)
_PUNTUACIO_EMPRESA = re.compile(r"[,\.;:\(\)\[\]\{\}\"\'\/\\&\+\*]")


def normalitzar_nom_persona(s: str) -> str:
    """
    Normalitza el nom d'una persona a Title Case amb partícules catalanes
    en minúscula quan apareixen al mig del nom.

    Gestiona:
      - Tot majúscules: "AINA PLAZA TESÍAS" → "Aina Plaza Tesías"
      - Tot minúscules: "teresa jordà i roura" → "Teresa Jordà i Roura"
      - Partícules: "i", "de", "del", "d'", "l'", etc.
      - Dobles espais i espais al voltant d'apòstrofs
    """
    if not s or not s.strip():
        return ""

    # Netejar espais múltiples i espais al voltant d'apòstrofs
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*'\s*", "'", s)

    # Separar per espais i apòstrofs (preservant-los)
    tokens = _tokenitzar_nom(s)

    result = []
    for i, token in enumerate(tokens):
        if not token:
            continue

        token_lower = token.lower()

        # Primera paraula sempre en majúscula
        if i == 0:
            result.append(_capitalitzar(token))
            continue

        # Partícules al mig → minúscula
        if token_lower in _PARTICULES_CA:
            result.append(token_lower)
        elif token_lower.rstrip("'") in _PARTICULES_CA:
            # "d'" o "l'" → minúscula preservant l'apòstrof
            result.append(token_lower)
        else:
            result.append(_capitalitzar(token))

    return " ".join(result)


def normalitzar_nom_empresa(s: str) -> str:
    """
    Genera una clau canonical d'empresa per a deduplicació.

    Transforma: "FUNDACIÓ HOSPITAL SANT JOAN DE DÉU, S.A." →
                "fundacio hospital sant joan de deu"

    No serveix per a mostrar al frontend; és per a matching intern.
    """
    if not s or not s.strip():
        return ""

    # Minúscules
    s = s.lower().strip()

    # Eliminar puntuació de formes jurídiques i general
    s = _PUNTUACIO_EMPRESA.sub(" ", s)

    # Eliminar formes jurídiques
    s = _FORMES_JURIDIQUES.sub(" ", s)

    # Eliminar accents i caràcters especials
    s = unidecode(s)

    # Netejar espais múltiples
    s = re.sub(r"\s+", " ", s).strip()

    return s


def _tokenitzar_nom(s: str) -> list[str]:
    """
    Divideix un nom en tokens preservant apòstrofs com a part del token.
    "Teresa d'Enginyeria" → ["Teresa", "d'Enginyeria"]
    """
    # Separar per espais
    parts = s.split(" ")
    return parts


def _capitalitzar(token: str) -> str:
    """
    Capitalitza la primera lletra d'un token, preservant la resta.
    Gestiona tokens amb apòstrof: "d'Alella" → "D'Alella" (si és primer)
    però "d'" sol → "d'" (partícula).
    """
    if not token:
        return token

    # Si conté apòstrof al principi (p. ex. "d'Avinyó"), capitalitzar la part post-apòstrof
    if "'" in token:
        parts = token.split("'", 1)
        pre = parts[0].lower()
        post = parts[1]
        if pre in _PARTICULES_CA and post:
            return f"{pre}'{post[0].upper()}{post[1:]}"

    return token[0].upper() + token[1:].lower() if len(token) > 1 else token.upper()
