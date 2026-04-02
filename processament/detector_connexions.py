"""
Detecció de connexions entre reunions i decisions posteriors.

ALGORISME v1.8:
  Per a cada reunió R amb embedding_tema E_r:
    1. Filtre protocol: dinar/inaugural/cloenda/festa/investidura/fires/visita+instal·lacions/etc.
    2. Filtre tema genèric: "Reunió de coordinació/seguiment", "Comitè organitzador", etc.
    3. Finestra temporal: [data_reunio, data_reunio + 180d]  ← CAUSAL: norma sempre posterior
    4. Cercar normes DOGC (excloent taxes anuals, suplem. crèdit, reestruc. dept.)
    5. Si similitud > THRESHOLD → CONNEXIÓ candidate
    6. Ajust dept: penalitza ×0.82 si depts. creuats (cap dels dos és GOVERN)
    7. Score 0-100: A(sim normalitzada) + B(temps) + C(dept) + D(import)
       Normalització LaBSE: [0.15→0, 0.50→50] — sostre real ~0.48 amb textos curts
       Penalització cross-dept: score_total ×0.80 si dept_match=False (max=66.4 < 70)
         → Empiricament: 0/32 TPs validats tenien dept_match=False
    8. Descartar score < 30
    9. Repetir per subvencions i contractes del grup

NOTES v1.9 (vs v1.8):
  - _RE_PROTOCOL: fix nit dels (Nit dels Economistes), +veredicte premis, +sant jordi,
      +jurat dels premis, +premi al reconeixement, +acte d'adhesions, +inici legislatura,
      +fòrum de FP/orientació/formació, +reunió president Generalitat
  - _RE_TEMA_GENERIC: +reunió presencial, +actuacions i projectes, +participació a l'acte,
      +presentar pla de treball, +presentació pla estratègic, +presentació de l'estratègia,
      +associacions professionals (genèric)
  - _RE_NORMA_RUTINA: +suplència del president (decrets administratius de suplències)

NOTES v1.11 (vs v1.10):
  - Calibrat amb 100 parells 0-10 (Pearson=0.610, F1=0.581 @ thr=70)
  - 42 FPs analitzats, patrons nous derivats de la mostra
  - _RE_PROTOCOL: fix adhesions (plural), fix inici curs (sense "de"),
      +entrega de diplomes, +consell d'administració (acte intern empresa),
      +roda de premsa, +llibre d'honor, +reunió del president de la Generalitat,
      +seminari (event extern com jornada)
  - _RE_TEMA_GENERIC: +activitats culturals/formatives/esportives,
      +premis nacionals/internacionals (tema=nom dels premis),
      +conèixer projectes/inversions, +col·laboracions i nous projectes,
      +projectes en curs, +per establir sinèrgies, +programa d'activitats,
      fix reunió telemàtica (extend virtual→telemàtica)
  - _RE_NORMA_RUTINA: ampliar currículum a ESO/batxillerat/primària/secundària

NOTES v1.10 (vs v1.9):
  - Calibrat amb escala 0-10 (100 parells, correlació Pearson=0.549)
  - F1 màxim a threshold=70 (0.600): prec=0.45, rec=0.90
  - Cross-dept: 0/35 positius (penalty ×0.80 ja els conté per sota de 70)
  - _RE_PROTOCOL: +presa de contacte, +war room, +acte de signatura de llei,
      +acompanya al conseller/director general, +recepció als clubs/participants
  - _RE_TEMA_GENERIC: +projectes i col·laboracions (ultra-genèric), +temes comuns,
      +presentació i temes comuns amb nova direcció, +posar en valor la col·laboració
      publico-privada, +consell català de X (òrgan consultiu formal),
      +comitè director, +assemblea general
  - _RE_NORMA_RUTINA: +règim electoral, +del currículum del cicle, +assignen als departaments
  - Keyword overlap bonus: +0.08 sim si >=2 paraules clau (>4 chars) compartides
      entre tema reunió i títol norma → recupera casos com "Taula Sectorial Porc"

THRESHOLD:          0.30 (calibrat per LaBSE: max real ~0.48)
WINDOW_BEFORE_DAYS: 0    (causal: la norma SEMPRE ha de ser posterior a la reunió)
WINDOW_AFTER_DAYS:  180  (6 mesos, finestra lobbying estàndard)
PRECISIO_OBJECTIU:  >60% TP a score >= 70

pgvector: <=> és distància cosinus. Similitud = 1 - distància.
"""

import logging
import os
import re
from sqlalchemy import text

logger = logging.getLogger(__name__)

THRESHOLD_SIMILITUD   = 0.30   # calibrat per LaBSE: màx real ~0.48, relació clara > 0.35
WINDOW_BEFORE_DAYS    = 0     # CAUSAL: only norms published AFTER meeting count as lobbying
WINDOW_AFTER_DAYS     = 180
VERSIO_ALGORISME      = "1.11.0"
MAX_NORMES_PER_REUNIO = 20

_USE_PGVECTOR = os.getenv("PGVECTOR_ENABLED", "false").lower() == "true"

# Reunions de protocol: no poden generar connexions reals de lobbying
_RE_PROTOCOL = re.compile(
    r"\b("
    r"dinar\b|sopar\b|[àa]pat\b|cocktail\b|c[òo]ctel\b|"
    r"inauguraci[oó]\b|inaugural\b|inaugura\b|"  # forma nominal i verbal
    r"investidura\b|"                             # investidura doctor honoris causa, etc.
    r"presentaci[oó]\s+(de\s+)?ll[ií]bre\b|"    # presentació de llibre / presentació llibre
    r"homenatge\b|gala\b|cerim[òo]nia\b|"
    r"concert\b|missa\b|"
    r"nit\s+(?:de\s+(?:l[a'\u2019]|les?\s+|el\s+)|dels?\s+)|"  # Nit de l'Empresa / Nit dels Economistes
    r"lliurament\s+(?:de[ls]?\s+|d['\u2019]\s*)?(?:\w+\s+)?premis\b|"  # "dels premis", "VI premis"
    r"entrega\s+de\s+(?:premis|diplomes)\b|"  # entrega de premis / entrega de diplomes
    r"lliurament\s+(de\s+la\s+|dels?\s+|de\s+les?\s+)(creu|medalla|placa|distinci|condecorac)\w*\b|"
    r"entrega\s+de\s+(les?\s+)?(flors?|honors?)\b|"
    r"\d+[\w\u00C0-\u024F]*\s*aniversari\b|"     # 50è aniversari, 30è aniversari...
    r"\d+\s+anys\b|centenari\b|"                 # 100 anys, 150 anys, centenari
    r"cloenda\b|"                                # cloenda de congrés/fòrum/acte
    r"inici\s+(?:de\s+)?curs\b|"                  # inici de curs / inici curs 2022-23
    r"congr[eé]s\b|"                             # congrés sectorial (entitat organitza/assisteix)
    r"seminari\b|"                               # seminari extern (com jornada, no lobbying directe)
    r"jornada\s+(?:de\s+|d['\u2019]\s*|sobre\s+|sectorial\b)|"  # "Jornada de/d'/sobre X", "II Jornada d'Y"
    r"acte\s+de\s+(?:mem[oò]ria|reconeixement)\b|"  # acte de memòria professional
    r"gran\s+premi\b|supercopa\b|super\s+copa\b|"  # competicions esportives (una o dues paraules)
    r"veredicte\s+(?:dels?\s+)?premis\b|"         # veredicte dels Premis X (acte jurat)
    r"jurat\s+(?:dels?\s+|de\s+la\s+(?:\d+\w*\s+)?edici[oó]\s+dels?\s+)?premis\b|"  # jurat dels Premis / jurat de la 5a edició dels Premis
    r"premi\s+(?:al?\s+|de\s+)?reconeixement\b|"  # Premi al Reconeixement (acte administratiu intern)
    r"acte\s+d['\u2019]adhesi[oó]ns?\b|"           # acte d'adhesió / acte d'adhesions (simbòlic)
    r"sant\s+jordi\b|"                             # celebració Sant Jordi (cultural)
    r"inici\s+(?:de\s+)?legislatura\b|"            # reunió d'inici de legislatura (protocol)
    r"f[oò]rum\s+(?:de\s+(?:formaci[oó]|fp|orientaci[oó])\b)|"  # Fòrum de Formació / FP / Orientació (interadministratiu)
    r"copa\s+(?:d['\u2019]|de\s+la\s+)(?:europa|el\s+m[oó]n|catalu)\b|"
    r"campionat\s+(?:d['\u2019]\s*|de\s+)(?:europa|el\s+m[oó]n|catalu\w*|espanya)\b|"
    r"sessi[oó]\s+informativa\b|"               # sessió informativa (entitat rep info)
    r"junta\s+(?:directiva|general)\b|"          # reunió de la junta directiva (acte intern)
    r"confer[èe]ncia\s+de\s+premsa|roda\s+de\s+premsa\b|"  # conferència / roda de premsa
    r"llibre\s+d['\u2019]honor\b|"              # signatura al Llibre d'Honor (cerimonial)
    r"consell\s+d['\u2019]administraci[oó]\b|"  # Consell d'Administració de X (acte intern empresa)
    r"reuni[oó]\s+(?:telem[àa]tica\s+)?del\s+president\s+(?:de\s+la\s+)?generalitat\b|"  # protocol màxim nivell
    r"acte\s+protocol|visita\s+protocol|visita\s+institucional|"
    r"visita\s+a\s+(les?\s+|l['\u2019]\s*)?instal·laci\w*\b|"  # visita a les instal·lacions
    r"festa\b|"                                  # Festa del Voluntariat, Festa Atletisme...
    r"fira\s+de\b|fires?\s+(sectorial|internacional|nacional|de\s+\w+)\b|"  # fires comercials
    r"expo\s+\w+\b|seafood\b|"                  # SeaFood Expo, Expo Barcelona...
    r"sal[oó]\s+(de\s+|del?\s+|d['\u2019])\w+\b|"  # Saló de l'Ensenyament, Saló Nàutic...
    r"f[oò]rum\s+europa\b|"                    # Fòrum Europa (acte extern, tribuna pública)
    r"presentaci[oó]\s+(del?\s+n[úu]mero|de\s+la\s+revista)\b|"  # presentació de revista/número
    r"visita\s+(?:una?\s+)?exposici[oó]\b|"    # Visita Exposició / Visita a una Exposició
    r"felicitaci[oó]\s+(de\s+)?nadal|"
    r"per\s+establir\s+sin[eè]rgies\b|"               # "per establir sinèrgies comunes" (qualsevol posició)
    r"primera\s+presa\s+de\s+contacte\b|"             # primera presa de contacte (presentació inicial)
    r"presa\s+de\s+contacte\b|"                        # presa de contacte (genèric protocol)
    r"war\s+room\b|"                                   # War Room COVID (gestió de crisi, no lobbying)
    r"acte\s+de\s+signatura\b|"                        # acte de signatura de llei/conveni (cerimònia)
    r"acompany[ae]\s+(?:al?\s+)?(?:conseller|directora?\s+general)\b|"  # acompanyar al conseller (protocol)
    r"recepci[oó]\s+(?:als?|a\s+la?|a\s+les?)\s+(?:clubs?|participants?|alumnes?|delegaci[oó])\b"  # recepció als clubs/participants
    r")",
    re.IGNORECASE,
)

# Temes de reunió massa genèrics: no permeten establir connexió semàntica real.
# Un tema ha d'identificar un assumpte concret per poder inferir influència lobbista.
_RE_TEMA_GENERIC = re.compile(
    r"^("
    # Salutacions i presentacions sense contingut
    r"reuni[oó]\s+institucional\.?\s*$|"
    r"(contacte|reuni[oó])\s+institucional\b|"
    r"presentaci[oó]\s+(i\s+seguiment\s+de\s+l.entitat|de\s+l.entitat|de\s+l.empresa|de\s+l.associaci[oó]|de\s+la\s+fundaci[oó])\b|"
    r"benvinguda\s+(al|a\s+la|als?)\s+(nou|nova|nou\s+conseller|nova\s+consellera)\b|"
    r"con[eè]ix(er|ement)\s+(el|la|els?|les?)\s+(nou|nova|director|directora|secretari|coordinador|responsable|delegat)\b|"
    r"(primera\s+)?reuni[oó]\s+de\s+con[eè]ixement\b|"
    r"presentar-se\s+i\s+presentar\b|"            # presentar-se i presentar-li X (protocol)
    # Temes del departament sense especificar
    r"temes\s+relacionats\s+amb\s+(el\s+|la\s+|els?\s+|les?\s+)?(secretari|departament|conseller|direcci[oó])\b|"
    r"temes\s+d.inter[eè]s\s+(del|de\s+la|de\s+l.)\s*(departament|secretari|consell|entitat)\b|"
    r"temes\s+generals?\s*$|"
    r"temes\s+a\s+tractar\s*$|"
    # Intercanvis genèrics sense contingut
    r"intercanvi\s+d.impressions\.?\s*$|"
    r"intercanvi\s+de\s+punts\s+de\s+vista\.?\s*$|"
    r"posada\s+en\s+com[úu]\s*$|"
    # Reunions de coordinació/seguiment sense tema específic
    # Sense anchor $ per capturar títols llargs com "Reunió de coordinació amb oficines tècniques"
    r"reuni[oó]\s+de\s+(coordinaci[oó]|control\s+(i\s+)?seguiment|seguiment|treball)\b|"
    r"reuni[oó]\s+peri[oò]dica\.?\s*$|"
    r"reuni[oó]\s+(?:virtual|telem[àa]tica)\b|"  # reunió virtual / telemàtica (sense tema real)
    r"reuni[oó]\s+presencial\b|"                 # reunió presencial a [adreça] (sense tema real)
    r"reuni[oó]\s+per\s+informar\b|"             # reunió per informar sobre X (no lobbying actiu)
    r"reuni[oó]\s+per\s+(?:a\s+)?tractar\b|"    # reunió per (a) tractar el conveni / temes genèrics
    r"revisar\s+i\s+tractar\b|"                  # revisar i tractar temes que afecten al sector
    # Actes de comitès organitzadors (esdeveniments externs, no lobbying directe)
    r"comit[eè]\s+organitzador\b|"
    # Tema que comença amb "Jornada X" (event extern, no reunió de pressió)
    r"jornada\b|"
    # Presentació interna de la junta de l'entitat (no lobbying)
    r"presentaci[oó]\s+(de\s+la\s+|del?\s+)?junta\b|"  # presentació de la junta (Col·legi)
    # Diàlegs electorals (actes polítics, no lobbying normatiu)
    r"di[àa]legs?\s+(?:amb\s+)?(?:els?\s+|les?\s+)?candidats\b|"
    # Visites a instal·lacions (protocol, no lobbying)
    r"visita\s+a\s+(les?\s+|l['\u2019]\s*)?instal·laci\w*\b|"
    # Temes o qüestions d'interès genèriques (sense contingut específic)
    r"q[üu]estions?\s+d['\u2019]inter[eè]s\s+(del?|de\s+la|de\s+l['\u2019])\s*(departament|secretari|consell|entitat)\b|"
    r"explorar\s+(possibles?\s+)?vies?\s+de\s+col·laboraci[oó]\b|"  # explorar vies col·lab (genèric)
    # Presentació genèrica de projectes/propostes sense especificació
    r"presentaci[oó]\s+(?:de\s+)?(?:nous?\s+|noves?\s+)?(?:projectes?|propostes?)\b\s*(?:de\s+col·laboraci[oó])?\s*$|"
    r"acta\s+de\s+presentaci[oó]\b|"            # acta de presentació de programa (genèric)
    r"presentaci[oó]\s+(de\s+la\s+|del?\s+)?secci[oó]\b|"  # presentació de la secció col·legial
    r"presentar?\s+(?:el\s+|la\s+)?pla\s+de\s+treball\b|"  # presentar el pla de treball (intern)
    r"presentaci[oó]\s+(?:del?\s+)?pla\s+estrat[eè]gic\b|"  # presentació del pla estratègic (extern)
    r"presentaci[oó]\s+de\s+l['\u2019]\s*estrat[eè]gia\b|"  # presentació de l'estratègia X
    r"actuacions\s+(i\s+)?(?:projectes?|activitats?)\b|"    # actuacions i projectes de la Fundació
    r"associacions?\s+professionals?\s*(?:,\s*empresarials?\s*(?:i\s+sindicals?)?)?\s*$|"  # associacions professionals... (ultra-genèric)
    r"^activitats\s+(?:culturals?|formatives?|esportives?)\s*$|"  # "Activitats Culturals" (ultra-genèric)
    r"^premis?\s+(?:nacionals?|internacionals?|de\s+(?:la\s+|l['\u2019]\s*)?\w)|"  # "Premis Nacionals de Cultura 2024" (acte de premis)
    r"con[eè]ixer\s+(?:(?:futurs?\s+|els?\s+|les?\s+)?projectes?|(?:les?\s+)?inversions?)\b|"  # "Conèixer futurs projectes d'inversió"
    r"^col·laboracions?\s+i\s+(?:nous?\s+)?projectes?\s*$|"  # "Col·laboracions i nous projectes"
    r"^projectes?\s+en\s+curs\s*$|"             # "Projectes en curs"
    r"^programa\s+d['\u2019]activitats\b|"      # "Programa d'activitats 2025" (calendari intern)
    # Assistència a events externs (l'entitat assisteix/participa, no fa lobbying directe)
    # Exclou taula/mesa/comissió/consell perquè aquells sí que podem ser lobbying
    r"assistència\s+(i\s+participaci[oó]\s+)?al?\s+"  # "a la/al/a l'"
    r"(la?\s+|l['\u2019]\s*|el\s+|al?\s+)?"
    r"(jornad(?:es|a)|congr(?:és|essos?)|fòrums?|debats?|seminaris?)\b|"
    r"assistència\s+(?:al?\s+l['\u2019]\s*|al?\s+)?acte\b|"  # assistència (a l')acte X (ceremonial)
    r"participaci[oó]\s+a\s+(?:l['\u2019]\s*|la\s+|el\s+)?acte\b|"  # participació a l'acte (extern)
    r"^projectes?\s+i\s+col[\.\-·]laboraci[oó]ns?\s*$|"             # "Projectes i col·laboracions" (ultra-genèric)
    r"^temes?\s+comuns?\b|"                                           # "Temes comuns" (sense especificació)
    r"^presentaci[oó]\s+(?:i\s+)?temes?\s+comuns?\b|"               # "Presentació i temes comuns amb nova direcció"
    r"posar\s+en\s+valor\s+la\s+col[\.\-·]laboraci[oó]\s+p[uú]blico[\-\s]privada\b|"  # col·lab publico-privada (genèric)
    r"^consell\s+catal[àa]\s+de\s+|"                                 # Consell Català de X (òrgan consultiu formal, no lobbying)
    r"^comit[eè]\s+director\b|"                                      # Comitè Director PAC (govern intern, no lobbying)
    r"^assemblee?s?\s+general\b"                                     # Assemblea General (acte intern entitat)
    r")",
    re.IGNORECASE,
)

# Normes rutinàries anuals: publicació automàtica sense relació amb lobbying concret.
# Inclou taxes/preus anuals I bases reguladores d'ajuts de minimis (publicades trimestralment
# per a cada sector agrari/industrial independentment de cap reunió específica).
_RE_NORMA_RUTINA = re.compile(
    r"("
    # Taxes i preus públics anuals
    r"preus?\s+p[úu]blics?\s+(per|que|del?)|"
    r"fixaci[oó]\s+dels?\s+preus?\s+p[úu]blics?|"
    r"taxes?\s+per\s+a\s+l[.']\s*any|"
    r"taxes?\s+per\s+a\s+el\s+curs|"
    r"publicaci[oó]\s+de\s+les?\s+taxes?|"
    r"dona\s+publicitat\s+a\s+les?\s+taxes?|"   # "per la qual es dona publicitat a les taxes"
    r"relaci[oó]\s+de\s+taxes?\s+vigents|"       # "relació de taxes vigents"
    r"taxes?\s+amb\s+car[àa]cter\s+general|"     # "taxes amb caràcter general"
    r"actualitzaci[oó]\s+(de\s+les?\s+)?taxes?|"
    r"import\s+de\s+les?\s+taxes?|"
    # Reestructuracions departamentals rutinàries (decisions internes, no influïdes per lobbies)
    r"reestructuraci[oó]\w*\s+(del?\s+|de\s+la\s+|de\s+l['\u2019])|"  # \w* per robustesa d'encoding
    # Suplències del president/conseller (decrets purament administratius)
    r"suplèn[cs]ia\s+(?:del?\s+|de\s+la\s+)president|"
    # Programes anuals d'actuació estadística
    r"programa\s+anual\s+d.actuaci[oó]\s+estad[ií]stica|"
    # Suplements de crèdit i crèdits extraordinaris (decisions pressupostàries, no lobbying)
    r"suplement\s+de\s+cr[eè]dit|"
    r"cr[eè]dit\s+extraordinari|"
    r"r[eè]gim\s+electoral\b|"                         # règim electoral (normes de cicle electoral)
    r"del\s+curr[íi]culum\s+del\s+cicle\b|"           # del currículum del cicle formatiu
    r"curr[íi]culum\s+de\s+(?:l['\u2019]\s*eso|batxillerat|prim[àa]ria|educaci[oó]\s+(?:prim|secund|infant)|l['\u2019]\s*ensenyament\s+secund)\b|"  # currículum de l'ESO/batxillerat/primària/etc.
    r"assignen\s+als?\s+departaments?\b"               # assignen als departaments (redistribució de recursos)
    r")",
    re.IGNORECASE,
)

# Cache de normes en memòria (carregat un cop per execució)
_normes_cache: list | None = None


def _cosine_sim(a, b) -> float:
    """Similitud cosinus entre dos arrays (numpy o llistes)."""
    import numpy as np
    a, b = np.array(a, dtype=np.float32), np.array(b, dtype=np.float32)
    na, nb = np.linalg.norm(a), np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


_STOP_KEYWORDS = {
    "dels", "les", "per", "que", "amb", "una", "uns", "dels", "del",
    "les", "els", "amb", "una", "uns", "sobre", "entre", "sense",
    "dins", "fins", "cada", "altre", "altres", "nova", "nous", "noves",
}

def _keyword_overlap_bonus(tema: str, titol_norma: str) -> float:
    """
    +0.08 a la similitud si >=2 paraules clau (>4 chars, no stopwords) són
    compartides entre el tema de la reunió i el títol de la norma.
    Recupera casos com "Taula Sectorial Porc" vs norma sobre sector porcí.
    """
    if not tema or not titol_norma:
        return 0.0
    import unicodedata

    def _tokenitzar(s: str) -> set:
        # Normalitzar accents i lowercase
        s_norm = unicodedata.normalize("NFD", s.lower())
        s_norm = "".join(c for c in s_norm if unicodedata.category(c) != "Mn")
        tokens = re.findall(r"[a-z]+", s_norm)
        return {t for t in tokens if len(t) > 4 and t not in _STOP_KEYWORDS}

    kw_tema = _tokenitzar(tema)
    kw_norma = _tokenitzar(titol_norma)
    shared = kw_tema & kw_norma
    return 0.08 if len(shared) >= 2 else 0.0


def _carregar_normes_cache(db) -> list:
    """Carrega les normes DOGC aptes per a connexions (exclou rutinàries)."""
    global _normes_cache
    if _normes_cache is not None:
        return _normes_cache
    rows = db.execute(text("""
        SELECT id, departament_codi, data_publicacio, titol,
               embedding_titol, embedding_resum
        FROM normativa_dogc
        WHERE embedding_titol IS NOT NULL
    """)).fetchall()
    # Excloure normes rutinàries (taxes, preus públics anuals)
    _normes_cache = [r for r in rows if not _RE_NORMA_RUTINA.search(r.titol or "")]
    n_total = len(rows)
    n_filtrades = n_total - len(_normes_cache)
    logger.info(
        f"[detector] Normes en cache: {len(_normes_cache)} "
        f"({n_filtrades} rutinàries excloses)"
    )
    return _normes_cache


def detectar_totes_connexions(db) -> dict:
    """
    Detecta connexions per a totes les reunions.
    Si hi ha connexions d'una versió anterior, les esborra i redetecta tot (clean run).
    Idempotent si ja hi ha connexions de la versió actual.
    """
    global _normes_cache
    stats = {"reunions": 0, "connexions": 0, "errors": 0}

    # Auto-reset: si existeixen connexions d'una versió diferent de l'actual, netejar.
    versio_db = db.execute(text(
        "SELECT versio_algorisme FROM connexions LIMIT 1"
    )).scalar()
    if versio_db is not None and versio_db != VERSIO_ALGORISME:
        logger.info(
            f"[detector] Connexions v{versio_db} detectades — esborrant per executar "
            f"v{VERSIO_ALGORISME} des de zero..."
        )
        db.execute(text("DELETE FROM connexions"))
        db.execute(text("DELETE FROM lobby_scores"))
        db.commit()
        _normes_cache = None  # invalidar cache
        logger.info("[detector] Reset complet. Redetectant totes les connexions.")

    select_embed = (
        "r.embedding_tema::text AS embed_str, NULL AS embedding_tema"
        if _USE_PGVECTOR else
        "NULL AS embed_str, r.embedding_tema"
    )
    reunions = db.execute(text(f"""
        SELECT r.id, r.data_reunio, r.departament_codi,
               r.tema_normalitzat,
               {select_embed},
               r.grup_id, r.carrec_id
        FROM reunions r
        WHERE r.embedding_tema IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM connexions c WHERE c.reunio_id = r.id
          )
        ORDER BY r.data_reunio DESC
    """)).fetchall()

    if not reunions:
        logger.info("[detector] Cap reunió pendent de processar.")
        return stats

    logger.info(f"[detector] Processant {len(reunions)} reunions...")

    for reunio in reunions:
        try:
            connexions = _buscar_connexions(db, reunio)
            for conn in connexions:
                _inserir_connexio(db, conn)
                stats["connexions"] += 1

            stats["reunions"] += 1
            if stats["reunions"] % 200 == 0:
                db.commit()
                logger.info(
                    f"  {stats['reunions']}/{len(reunions)} reunions — "
                    f"{stats['connexions']} connexions detectades"
                )

        except Exception as e:
            logger.error(f"Error reunió id={reunio.id}: {e}", exc_info=True)
            stats["errors"] += 1
            db.rollback()

    db.commit()
    logger.info(
        f"[detector] Completat: {stats['reunions']} reunions, "
        f"{stats['connexions']} connexions, {stats['errors']} errors"
    )
    return stats


def detectar_connexions_reunio(db, reunio_id: int) -> list[dict]:
    """
    Detecta connexions per a una reunió concreta (útil per a re-processament puntual).
    """
    select_embed = (
        "embedding_tema::text AS embed_str, NULL AS embedding_tema"
        if _USE_PGVECTOR else
        "NULL AS embed_str, embedding_tema"
    )
    reunio = db.execute(text(f"""
        SELECT id, data_reunio, departament_codi, tema_normalitzat,
               {select_embed}, grup_id, carrec_id
        FROM reunions WHERE id = :id
    """), {"id": reunio_id}).fetchone()

    if not reunio or (reunio.embed_str is None and reunio.embedding_tema is None):
        return []

    return _buscar_connexions(db, reunio)


def _buscar_connexions(db, reunio) -> list[dict]:
    """Cerca connexions per a una reunió (pgvector o numpy fallback)."""
    tema = getattr(reunio, "tema_normalitzat", "") or ""

    # Reunions de protocol no generen connexions de lobbying
    if _RE_PROTOCOL.search(tema):
        return []

    # Temes massa genèrics: impossibles d'associar a una norma específica
    if _RE_TEMA_GENERIC.search(tema):
        return []

    if _USE_PGVECTOR:
        return _buscar_connexions_pgvector(db, reunio)
    return _buscar_connexions_numpy(db, reunio)


def _ajustar_similitud(sim: float, dept_reunio: str, dept_norma: str) -> float:
    """
    Ajust de similitud per coincidència de departament.

    Lògica:
    - Mateix dept (i no GOVERN): neutre — la similitud parla per si sola
    - Dept diferent (cap dels dos GOVERN): penalitza ×0.82
      (connexions cross-dept existeixen però són menys probables)
    - Qualsevol és GOVERN: neutre (actes transversals, no imputables a un dept)
    """
    CODIS_GENERICS = (None, "DESCONEGUT", "GOVERN")
    if dept_reunio in CODIS_GENERICS or dept_norma in CODIS_GENERICS:
        return sim
    if dept_reunio == dept_norma:
        return sim
    return sim * 0.82  # penalitza cross-dept


def _embed_de_reunio(reunio) -> list | None:
    """Extreu l'embedding com a llista Python."""
    # Quan PGVECTOR_ENABLED=false, fetchall retorna REAL[] com a llista
    # Quan s'usa embed_str (::text), cal parsejar
    emb = getattr(reunio, "embedding_tema", None)
    if emb is None:
        embed_str = getattr(reunio, "embed_str", None)
        if not embed_str:
            return None
        # Parsejar string PostgreSQL "{1.2,3.4,...}"
        return [float(x) for x in embed_str.strip("{}").split(",")]
    return list(emb)


def _buscar_connexions_numpy(db, reunio) -> list[dict]:
    """Cerca connexions usant similitud cosinus en Python (fallback sense pgvector)."""
    import datetime
    connexions = []
    embed_r = _embed_de_reunio(reunio)
    if not embed_r:
        return []

    data_r = reunio.data_reunio
    data_min = data_r - datetime.timedelta(days=WINDOW_BEFORE_DAYS)
    data_max = data_r + datetime.timedelta(days=WINDOW_AFTER_DAYS)

    # Normes en finestra temporal
    normes_candidats = [
        n for n in _carregar_normes_cache(db)
        if data_min <= n.data_publicacio <= data_max and n.embedding_titol is not None
    ]

    tema = reunio.tema_normalitzat or ""
    scored = []
    for norma in normes_candidats:
        sim_t = _cosine_sim(embed_r, norma.embedding_titol)
        sim_r = _cosine_sim(embed_r, norma.embedding_resum) if norma.embedding_resum else 0.0
        sim = max(sim_t, sim_r)
        sim += _keyword_overlap_bonus(tema, norma.titol or "")
        if sim >= THRESHOLD_SIMILITUD:
            scored.append((sim, norma))

    scored.sort(key=lambda x: x[0], reverse=True)
    scored = scored[:MAX_NORMES_PER_REUNIO]

    for sim, norma in scored:
        dept_match = (
            norma.departament_codi == reunio.departament_codi
            and norma.departament_codi not in (None, "DESCONEGUT", "GOVERN")
        )
        sim_aj = _ajustar_similitud(sim, reunio.departament_codi, norma.departament_codi)
        dies = (norma.data_publicacio - data_r).days
        connexions.append({
            "reunio_id":                  reunio.id,
            "tipus_decisio":              "normativa_dogc",
            "decisio_normativa_id":       norma.id,
            "decisio_subvencio_id":       None,
            "decisio_contracte_id":       None,
            "dies_entre_reunio_decisio":  dies,
            "similitud_semantica":        round(sim_aj, 4),
            "similitud_departament":      dept_match,
            "connexio_score": calcular_score_connexio(sim_aj, dies, dept_match),
        })

    return connexions


def _buscar_connexions_pgvector(db, reunio) -> list[dict]:
    """Cerca connexions per a una reunió via pgvector (<=> operator)."""
    connexions = []
    embed_str = reunio.embed_str

    tema = reunio.tema_normalitzat or ""

    # ---- DOGC ----
    normes = db.execute(text(f"""
        SELECT
            n.id,
            n.titol,
            n.departament_codi,
            n.data_publicacio,
            1 - (n.embedding_titol <=> CAST(:embed AS vector))  AS sim_titol,
            1 - (n.embedding_resum  <=> CAST(:embed AS vector))  AS sim_resum
        FROM normativa_dogc n
        WHERE
            n.data_publicacio BETWEEN
                CAST(:data_reunio AS date) - INTERVAL '{WINDOW_BEFORE_DAYS} days'
                AND
                CAST(:data_reunio AS date) + INTERVAL '{WINDOW_AFTER_DAYS} days'
            AND n.embedding_titol IS NOT NULL
        ORDER BY n.embedding_titol <=> CAST(:embed AS vector)
        LIMIT {MAX_NORMES_PER_REUNIO}
    """), {
        "embed":       embed_str,
        "data_reunio": str(reunio.data_reunio),
    }).fetchall()

    for norma in normes:
        sim = max(float(norma.sim_titol or 0), float(norma.sim_resum or 0))
        sim += _keyword_overlap_bonus(tema, norma.titol or "")
        if sim < THRESHOLD_SIMILITUD:
            continue

        dept_match = (
            norma.departament_codi == reunio.departament_codi
            and norma.departament_codi not in (None, "DESCONEGUT", "GOVERN")
        )
        sim_aj = _ajustar_similitud(sim, reunio.departament_codi, norma.departament_codi)
        dies = (norma.data_publicacio - reunio.data_reunio).days

        connexions.append({
            "reunio_id":                  reunio.id,
            "tipus_decisio":              "normativa_dogc",
            "decisio_normativa_id":       norma.id,
            "decisio_subvencio_id":       None,
            "decisio_contracte_id":       None,
            "dies_entre_reunio_decisio":  dies,
            "similitud_semantica":        round(sim_aj, 4),
            "similitud_departament":      dept_match,
            "connexio_score": calcular_score_connexio(sim_aj, dies, dept_match),
        })

    # ---- Subvencions (si la reunió té grup assignat) ----
    if reunio.grup_id:
        subvencions = db.execute(text(f"""
            SELECT
                s.id,
                s.departament_codi,
                s.data_concessio,
                s.import_euros,
                1 - (s.embedding_finalitat <=> CAST(:embed AS vector)) AS sim
            FROM subvencions_lobby s
            WHERE
                s.grup_id = :grup_id
                AND s.data_concessio BETWEEN
                    CAST(:data_reunio AS date) - INTERVAL '{WINDOW_BEFORE_DAYS} days'
                    AND
                    CAST(:data_reunio AS date) + INTERVAL '{WINDOW_AFTER_DAYS} days'
                AND s.embedding_finalitat IS NOT NULL
            ORDER BY s.embedding_finalitat <=> CAST(:embed AS vector)
            LIMIT 5
        """), {
            "embed":       embed_str,
            "grup_id":     reunio.grup_id,
            "data_reunio": str(reunio.data_reunio),
        }).fetchall()

        for s in subvencions:
            sim = float(s.sim or 0)
            if sim < THRESHOLD_SIMILITUD - 0.10:
                continue
            dept_match = (s.departament_codi == reunio.departament_codi
                          and s.departament_codi not in (None, "DESCONEGUT"))
            dies = (s.data_concessio - reunio.data_reunio).days
            connexions.append({
                "reunio_id":                 reunio.id,
                "tipus_decisio":             "subvencio",
                "decisio_normativa_id":      None,
                "decisio_subvencio_id":      s.id,
                "decisio_contracte_id":      None,
                "dies_entre_reunio_decisio": dies,
                "similitud_semantica":       round(sim, 4),
                "similitud_departament":     dept_match,
                "connexio_score": calcular_score_connexio(
                    sim, dies, dept_match, import_euros=float(s.import_euros or 0)
                ),
            })

    # ---- Acords del Govern ----
    acords = db.execute(text(f"""
        SELECT
            a.id,
            a.titol,
            a.departament_codi,
            a.data_sessio,
            1 - (a.embedding_titol <=> CAST(:embed AS vector)) AS sim
        FROM acords_govern a
        WHERE
            a.data_sessio BETWEEN
                CAST(:data_reunio AS date) - INTERVAL '{WINDOW_BEFORE_DAYS} days'
                AND
                CAST(:data_reunio AS date) + INTERVAL '{WINDOW_AFTER_DAYS} days'
            AND a.embedding_titol IS NOT NULL
        ORDER BY a.embedding_titol <=> CAST(:embed AS vector)
        LIMIT {MAX_NORMES_PER_REUNIO}
    """), {
        "embed":       embed_str,
        "data_reunio": str(reunio.data_reunio),
    }).fetchall()

    for acord in acords:
        sim = float(acord.sim or 0)
        sim += _keyword_overlap_bonus(tema, acord.titol or "")
        if sim < THRESHOLD_SIMILITUD:
            continue

        dept_match = (
            acord.departament_codi == reunio.departament_codi
            and acord.departament_codi not in (None, "DESCONEGUT", "GOVERN")
        )
        sim_aj = _ajustar_similitud(sim, reunio.departament_codi, acord.departament_codi)
        dies = (acord.data_sessio - reunio.data_reunio).days

        connexions.append({
            "reunio_id":                  reunio.id,
            "tipus_decisio":              "acord_govern",
            "decisio_normativa_id":       None,
            "decisio_subvencio_id":       None,
            "decisio_contracte_id":       None,
            "decisio_acord_govern_id":    acord.id,
            "dies_entre_reunio_decisio":  dies,
            "similitud_semantica":        round(sim_aj, 4),
            "similitud_departament":      dept_match,
            "connexio_score": calcular_score_connexio(sim_aj, dies, dept_match),
        })

    # Filtrar connexions per sota del llindar mínim de score útil
    connexions = [c for c in connexions if c["connexio_score"] >= 30.0]

    # Ordenar per score descendent
    connexions.sort(key=lambda c: c["connexio_score"], reverse=True)

    return connexions


def calcular_score_connexio(
    similitud: float,
    dies: int,
    dept_match: bool,
    import_euros: float = 0.0,
) -> float:
    """
    Score de la connexió entre una reunió i una decisió. Rang 0-100.

    Components:
      A. Similitud semàntica  (0-50): component principal
      B. Proximitat temporal  (0-30): més proper = més significatiu
      C. Coincidència dept.   (0-15): mateix organisme → connexió directa
      D. Import econòmic      (0-5):  si hi ha diners de per mig

    Calibrar amb:
      - 10 connexions evidents (reunió sobre X, decret sobre X pocs dies després)
      - 10 connexions aleatòries (temes sense relació)
    Objectiu: true positives > 80% per sobre del llindar escollit.
    """
    # A. Similitud semàntica
    # LaBSE en textos curts (meetings + títols DOGC) té sostre real ~0.50.
    # Normalitzem [0.15 → 0, 0.50 → 50] per aprofitar tot el rang del score.
    SIM_MIN, SIM_MAX = 0.15, 0.50
    sim_norm = max(0.0, (similitud - SIM_MIN) / (SIM_MAX - SIM_MIN))
    score_sim = min(sim_norm * 50.0, 50.0)

    # B. Proximitat temporal
    # Norma DESPRÉS de la reunió (reunió influeix en la decisió — senyal fort)
    # Norma ABANS de la reunió (reunió de seguiment/informació — senyal feble)
    if dies < 0:
        abs_dies = abs(dies)
        if abs_dies <= 30:
            score_temps = 22.0   # molt recent: reunió de seguiment
        elif abs_dies <= 60:
            score_temps = 16.0
        elif abs_dies <= 90:
            score_temps = 10.0
        else:                    # 91-120 dies: influència anticipada
            score_temps = 5.0
    elif dies <= 14:
        score_temps = 30.0       # decisió immediata post-reunió: màxim
    elif dies <= 30:
        score_temps = 26.0
    elif dies <= 60:
        score_temps = 20.0
    elif dies <= 90:
        score_temps = 14.0
    elif dies <= 180:
        score_temps = 8.0
    else:
        score_temps = 3.0

    # C. Departament
    score_dept = 15.0 if dept_match else 3.0

    # D. Import econòmic
    if import_euros >= 1_000_000:
        score_import = 5.0
    elif import_euros >= 100_000:
        score_import = 3.0
    elif import_euros > 0:
        score_import = 1.0
    else:
        score_import = 0.0

    total = score_sim + score_temps + score_dept + score_import

    # Penalització cross-dept: si dept_match=False, el total es redueix un 20%.
    # Fonament empíric: en 300+ parelles validades, 0/32 TPs tenien dept_match=False.
    # Efecte: score màxim sense dept = (50+30+3)*0.80 = 66.4 → no supera el llindar 70.
    if not dept_match:
        total = total * 0.80

    return min(round(total, 1), 100.0)


def _inserir_connexio(db, conn: dict) -> None:
    """Insereix una connexió nova (ON CONFLICT DO NOTHING per idempotència)."""
    db.execute(text("""
        INSERT INTO connexions (
            reunio_id, tipus_decisio,
            decisio_normativa_id, decisio_subvencio_id,
            decisio_contracte_id, decisio_acord_govern_id,
            dies_entre_reunio_decisio, similitud_semantica,
            similitud_departament, connexio_score, versio_algorisme
        ) VALUES (
            :reunio_id, :tipus,
            :norm_id, :subv_id, :cont_id, :acord_id,
            :dies, :sim, :dept_match, :score, :versio
        )
        ON CONFLICT DO NOTHING
    """), {
        "reunio_id":  conn["reunio_id"],
        "tipus":      conn["tipus_decisio"],
        "norm_id":    conn.get("decisio_normativa_id"),
        "subv_id":    conn.get("decisio_subvencio_id"),
        "cont_id":    conn.get("decisio_contracte_id"),
        "acord_id":   conn.get("decisio_acord_govern_id"),
        "dies":       conn["dies_entre_reunio_decisio"],
        "sim":        conn["similitud_semantica"],
        "dept_match": conn["similitud_departament"],
        "score":      conn["connexio_score"],
        "versio":     VERSIO_ALGORISME,
    })
