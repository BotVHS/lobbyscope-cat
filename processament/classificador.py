"""
Classificació LLM de les connexions detectades.

Per a cada connexió d'alt score (>= SCORE_MINIM_LLM), Claude genera:
  - Si hi ha connexió temàtica real (true/false + confiança)
  - Una narrativa factual en català de 2-3 frases
  - Els factors que connecten les dues entitats
  - Un flag si recomana revisió humana

COST ESTIMAT:
  ~600 tokens per crida (prompt + resposta)
  Assumint 5.000 connexions d'alt score: ~$0.15 amb claude-haiku-4-5

MODEL RECOMANAT:
  claude-haiku-4-5-20251001 — ràpid i econòmic per a tasques de classificació
  Canviar a claude-sonnet-4-6 per a casos d'alta visibilitat pública.

DISCLAIMER OBLIGATORI:
  Totes les narratives han d'acabar amb el text estàndard de disclamer
  (veure DISCLAMER_CA) que aclareix que les connexions són estadístiques
  i no impliquen cap relació causal ni irregularitat.
"""

import json
import logging
import os

import anthropic

logger = logging.getLogger(__name__)

MODEL_LLM       = "claude-haiku-4-5-20251001"
SCORE_MINIM_LLM = 60.0   # només classificar connexions d'alt score

DISCLAMER_CA = (
    "Aquesta coincidència és estadística i no implica necessàriament "
    "cap relació causal ni irregularitat."
)

PROMPT_CONNEXIO = """Ets un analista de transparència institucional. Analitza si hi ha una connexió temàtica plausible entre la reunió i la decisió pública.

REUNIÓ:
- Data: {data_reunio}
- Càrrec: {nom_carrec} ({titol_carrec}), {departament}
- Grup d'interès: {nom_grup}
- Tema declarat: "{tema}"

DECISIÓ POSTERIOR ({dies} dies):
- Tipus: {tipus_decisio}
- Títol: "{titol_decisio}"
- Departament: {dept_decisio}
- Data: {data_decisio}

Retorna ÚNICAMENT JSON vàlid sense cap text addicional:
{{
  "connexio_tematica": true,
  "confianca": 0.85,
  "tema_comu": "descripció breu del tema comú",
  "factors_connexio": ["factor 1", "factor 2"],
  "factors_dubte": ["per qué podria ser coincidència"],
  "narrativa_ca": "Frase 1 (qui, quan, sobre qué). Frase 2 (quina decisió, quan). Frase 3 (vincle). {disclamer}",
  "recomanacio_revisio": false
}}"""


_client: anthropic.Anthropic | None = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key or api_key.startswith("sk-ant-..."):
            raise ValueError(
                "ANTHROPIC_API_KEY no configurada. "
                "Afegir-la al fitxer .env per activar la classificació LLM."
            )
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


def classificar_connexions_pendents(db) -> dict:
    """
    Classifica via LLM totes les connexions d'alt score sense narrativa.
    """
    from sqlalchemy import text

    connexions = db.execute(text("""
        SELECT
            c.id,
            c.connexio_score,
            c.tipus_decisio,
            c.dies_entre_reunio_decisio,
            c.decisio_normativa_id,
            c.decisio_subvencio_id,
            r.data_reunio,
            r.tema_original,
            r.departament,
            rc.nom_canonical  AS nom_carrec,
            rc.titol          AS titol_carrec,
            g.nom             AS nom_grup
        FROM connexions c
        JOIN reunions r       ON r.id  = c.reunio_id
        LEFT JOIN carrecs rc  ON rc.id = r.carrec_id
        LEFT JOIN grups g     ON g.id  = r.grup_id
        WHERE c.explicacio_ca IS NULL
          AND c.connexio_score >= :score_min
        ORDER BY c.connexio_score DESC
    """), {"score_min": SCORE_MINIM_LLM}).fetchall()

    if not connexions:
        logger.info("[classificador] Cap connexió pending de classificar.")
        return {"processades": 0, "errors": 0}

    stats = {"processades": 0, "errors": 0}
    logger.info(f"[classificador] Classificant {len(connexions)} connexions...")

    for conn in connexions:
        try:
            decisio = _obtenir_decisio(db, conn)
            resultat = classificar_connexio(conn, decisio)
            _guardar_classificacio(db, conn.id, resultat)
            stats["processades"] += 1

            if stats["processades"] % 50 == 0:
                db.commit()
                logger.info(f"  {stats['processades']}/{len(connexions)} classificades")

        except ValueError as e:
            # API key no configurada — aturar el procés
            logger.error(f"[classificador] {e}")
            break
        except Exception as e:
            logger.error(f"Error classificant connexió {conn.id}: {e}")
            stats["errors"] += 1

    db.commit()
    logger.info(f"[classificador] Completat: {stats}")
    return stats


def classificar_connexio(conn, decisio: dict) -> dict:
    """
    Classifica una connexió via Claude. Retorna el diccionari de resultats.
    Si l'API falla, retorna un resultat per defecte amb flag de revisió.
    """
    prompt = PROMPT_CONNEXIO.format(
        data_reunio=str(getattr(conn, "data_reunio", "")),
        nom_carrec=getattr(conn, "nom_carrec", "") or "Desconegut",
        titol_carrec=getattr(conn, "titol_carrec", "") or "",
        departament=getattr(conn, "departament", "") or "",
        nom_grup=getattr(conn, "nom_grup", "") or "Desconegut",
        tema=(getattr(conn, "tema_original", "") or "")[:400],
        dies=getattr(conn, "dies_entre_reunio_decisio", "?"),
        tipus_decisio=getattr(conn, "tipus_decisio", ""),
        titol_decisio=(decisio.get("titol") or "")[:300],
        dept_decisio=decisio.get("departament") or "",
        data_decisio=decisio.get("data") or "",
        disclamer=DISCLAMER_CA,
    )

    try:
        client = _get_client()
        resp = client.messages.create(
            model=MODEL_LLM,
            max_tokens=700,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()

        # Netejar markdown code fences si n'hi ha
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) >= 2 else raw
            if raw.startswith("json"):
                raw = raw[4:].strip()

        return json.loads(raw)

    except (json.JSONDecodeError, IndexError) as e:
        logger.warning(f"Resposta LLM no parsejable: {e}")
        return _resultat_per_defecte()
    except Exception as e:
        logger.error(f"Error crida LLM: {e}")
        return _resultat_per_defecte()


def _obtenir_decisio(db, conn) -> dict:
    """Carrega les dades de la decisió associada a la connexió."""
    from sqlalchemy import text

    if conn.tipus_decisio == "normativa_dogc" and conn.decisio_normativa_id:
        row = db.execute(text("""
            SELECT titol, departament, data_publicacio::text AS data
            FROM normativa_dogc WHERE id = :id
        """), {"id": conn.decisio_normativa_id}).fetchone()
        return dict(row._mapping) if row else {}

    if conn.tipus_decisio == "subvencio" and conn.decisio_subvencio_id:
        row = db.execute(text("""
            SELECT finalitat AS titol, departament, data_concessio::text AS data
            FROM subvencions_lobby WHERE id = :id
        """), {"id": conn.decisio_subvencio_id}).fetchone()
        return dict(row._mapping) if row else {}

    return {}


def _guardar_classificacio(db, connexio_id: int, resultat: dict) -> None:
    """Guarda el resultat de la classificació LLM a la connexió."""
    from sqlalchemy import text

    narrativa = resultat.get("narrativa_ca") or ""
    # Assegurar que el disclamer és present a la narrativa
    if narrativa and DISCLAMER_CA not in narrativa:
        narrativa += " " + DISCLAMER_CA

    factors = resultat.get("factors_connexio") or []
    if isinstance(factors, list):
        factors_arr = factors
    else:
        factors_arr = []

    db.execute(text("""
        UPDATE connexions
        SET explicacio_ca   = :narrativa,
            factors_connexio = :factors,
            es_connexio_valida = :valida,
            revisat_manualment = false
        WHERE id = :id
    """), {
        "narrativa": narrativa[:2000] if narrativa else None,
        "factors":   factors_arr,
        "valida":    resultat.get("connexio_tematica"),
        "id":        connexio_id,
    })


def _resultat_per_defecte() -> dict:
    return {
        "connexio_tematica":  None,
        "confianca":          0.5,
        "tema_comu":          "",
        "factors_connexio":   [],
        "factors_dubte":      ["No s'ha pogut generar la classificació automàtica."],
        "narrativa_ca":       f"Connexió pendent de revisió manual. {DISCLAMER_CA}",
        "recomanacio_revisio": True,
    }
