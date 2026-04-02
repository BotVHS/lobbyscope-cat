"""
Càlcul del Lobby Influence Score per a cada grup d'interès.

COMPONENTS (total 0-100):
  A. Freqüència de reunions   (0-25): quantes reunions ha tingut
  B. Diversitat de càrrecs    (0-25): amb quants càrrecs/departaments
  C. Connexions a decisions   (0-30): quantes decisions han seguit reunions
  D. Valor econòmic rebut     (0-20): imports de subvencions/contractes

INTERPRETACIÓ DEL SCORE:
  0-20:   presència baixa o puntual
  21-40:  activitat lobbista moderada
  41-60:  lobby actiu amb presència regular
  61-80:  lobby molt actiu amb influència documentada
  81-100: lobby d'alta influència (top 1% del registre)

VERSIÓ: 1.0.0 — documentar al changelog si es modifica l'algorisme
"""

import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)

VERSIO_ALGORISME = "1.2.0"

# Llindar mínim de connexio_score per comptar com a connexió de qualitat.
# Calibrat per LaBSE: score >= 70 vol dir sim > 0.44 + temps + dept — connexió sòlida.
CONNEXIO_SCORE_MIN = 70.0


def recalcular_tots_scores(db) -> dict:
    """Recalcula el score per a tots els grups amb activitat."""
    grups = db.execute(text("""
        SELECT DISTINCT grup_id FROM reunions WHERE grup_id IS NOT NULL
    """)).fetchall()

    stats = {"processats": 0, "errors": 0}
    for row in grups:
        try:
            actualitzar_score_grup(db, row.grup_id)
            stats["processats"] += 1
            if stats["processats"] % 200 == 0:
                db.commit()
                logger.info(f"[scores] {stats['processats']} grups processats")
        except Exception as e:
            logger.error(f"Error score grup {row.grup_id}: {e}")
            stats["errors"] += 1
            db.rollback()

    db.commit()
    logger.info(f"[scores] Completat: {stats}")
    return stats


def actualitzar_score_grup(db, grup_id: int) -> dict:
    """
    Calcula i guarda el Lobby Influence Score per a un grup.
    Retorna el diccionari de components del score.
    """
    dades = _obtenir_dades_grup(db, grup_id)
    if not dades or dades["total_reunions"] == 0:
        return {}

    score_freq  = _score_frequencia(dades["total_reunions"])
    score_div   = _score_diversitat(
        dades["carrecs_diferents"],
        dades["departaments_diferents"],
        dades["total_reunions"],
    )
    score_conn  = _score_connexions(dades["total_connexions"], dades["total_reunions"])
    score_valor = _score_valor_economic(dades["import_total"])

    score_total = min(score_freq + score_div + score_conn + score_valor, 100.0)

    result = {
        "grup_id":                  grup_id,
        "score_total":              round(score_total, 1),
        "score_frequencia":         score_freq,
        "score_diversitat_carrecs": score_div,
        "score_connexio_decisions": score_conn,
        "score_valor_economic":     score_valor,
        "total_reunions":           dades["total_reunions"],
        "total_carrecs_contactats": dades["carrecs_diferents"],
        "total_connexions":         dades["total_connexions"],
        "import_total_rebut":       dades["import_total"],
        "primera_reunio":           dades["primera_reunio"],
        "ultima_reunio":            dades["ultima_reunio"],
        "versio_algorisme":         VERSIO_ALGORISME,
    }

    _upsert_score(db, result)
    return result


def _obtenir_dades_grup(db, grup_id: int) -> dict:
    row = db.execute(text("""
        SELECT
            COUNT(DISTINCT r.id)               AS total_reunions,
            COUNT(DISTINCT r.carrec_id)        AS carrecs_diferents,
            COUNT(DISTINCT r.departament_codi)
                FILTER (WHERE r.departament_codi NOT IN ('GOVERN','DESCONEGUT'))
                                               AS departaments_diferents,
            MIN(r.data_reunio)                 AS primera_reunio,
            MAX(r.data_reunio)                 AS ultima_reunio,
            COUNT(DISTINCT c.reunio_id)
                FILTER (WHERE c.connexio_score >= :score_min)
                                               AS total_connexions,
            (SELECT COALESCE(SUM(import_euros), 0)
               FROM subvencions_lobby
              WHERE grup_id = :grup_id)        AS import_total
        FROM reunions r
        LEFT JOIN connexions c ON c.reunio_id = r.id
        WHERE r.grup_id = :grup_id
    """), {"grup_id": grup_id, "score_min": CONNEXIO_SCORE_MIN}).fetchone()

    if not row:
        return {}

    return {
        "total_reunions":       int(row.total_reunions or 0),
        "carrecs_diferents":    int(row.carrecs_diferents or 0),
        "departaments_diferents": int(row.departaments_diferents or 0),
        "total_connexions":     int(row.total_connexions or 0),
        "import_total":         float(row.import_total or 0),
        "primera_reunio":       row.primera_reunio,
        "ultima_reunio":        row.ultima_reunio,
    }


def _score_frequencia(total_reunions: int) -> float:
    """Freqüència de reunions (0-25)."""
    if total_reunions >= 50:  return 25.0
    if total_reunions >= 30:  return 22.0
    if total_reunions >= 20:  return 18.0
    if total_reunions >= 10:  return 13.0
    if total_reunions >= 5:   return 9.0
    if total_reunions >= 2:   return 5.0
    return 2.0


def _score_diversitat(
    carrecs_diferents: int,
    departaments_diferents: int,
    total_reunions: int,
) -> float:
    """
    Diversitat de l'accés (0-25): càrrecs + departaments contactats.

    Un lobby que accedeix a càrrecs de múltiples departaments té una
    xarxa d'influència més àmplia. Combina dos senyals:
      - Diversitat de càrrecs (2/3 del pes): cap a quantes persones
      - Diversitat de departaments (1/3 del pes): cap a quants òrgans

    Escala: 10 càrrecs / 3 depts → ~25 punts (màxim amb 20+ reunions)
    """
    if total_reunions == 0:
        return 0.0
    # Càrrecs: ratio ideal ~0.4 (contactes variats però possible repetició)
    score_carrecs = min((carrecs_diferents / total_reunions) * 40.0, 17.0)
    # Departaments: cada dept nou val 2 punts, fins a 8 punts màxims
    score_depts   = min(departaments_diferents * 2.0, 8.0)
    return round(score_carrecs + score_depts, 1)


def _score_connexions(total_connexions: int, total_reunions: int) -> float:
    """Connexions detectades a decisions posteriors (0-30)."""
    if total_reunions == 0:
        return 0.0
    if total_connexions == 0:
        return 0.0
    taxa = total_connexions / total_reunions
    score = min(taxa * 60.0, 30.0)
    return round(score, 1)


def _score_valor_economic(import_total: float) -> float:
    """Valor econòmic rebut en subvencions/contractes (0-20)."""
    if import_total >= 50_000_000:  return 20.0
    if import_total >= 10_000_000:  return 17.0
    if import_total >= 1_000_000:   return 13.0
    if import_total >= 100_000:     return 8.0
    if import_total >= 10_000:      return 4.0
    if import_total > 0:            return 1.0
    return 0.0


def _upsert_score(db, result: dict) -> None:
    db.execute(text("""
        INSERT INTO lobby_scores (
            grup_id, score_total, score_frequencia, score_diversitat_carrecs,
            score_connexio_decisions, score_valor_economic,
            total_reunions, total_carrecs_contactats, total_connexions,
            import_total_rebut, primera_reunio, ultima_reunio,
            versio_algorisme, calculat_at
        ) VALUES (
            :grup_id, :score_total, :score_freq, :score_div,
            :score_conn, :score_valor,
            :total_reunions, :total_carrecs, :total_connexions,
            :import_total, :primera_reunio, :ultima_reunio,
            :versio, NOW()
        )
        ON CONFLICT (grup_id) DO UPDATE SET
            score_total               = EXCLUDED.score_total,
            score_frequencia          = EXCLUDED.score_frequencia,
            score_diversitat_carrecs  = EXCLUDED.score_diversitat_carrecs,
            score_connexio_decisions  = EXCLUDED.score_connexio_decisions,
            score_valor_economic      = EXCLUDED.score_valor_economic,
            total_reunions            = EXCLUDED.total_reunions,
            total_carrecs_contactats  = EXCLUDED.total_carrecs_contactats,
            total_connexions          = EXCLUDED.total_connexions,
            import_total_rebut        = EXCLUDED.import_total_rebut,
            primera_reunio            = EXCLUDED.primera_reunio,
            ultima_reunio             = EXCLUDED.ultima_reunio,
            versio_algorisme          = EXCLUDED.versio_algorisme,
            calculat_at               = NOW()
    """), {
        "grup_id":      result["grup_id"],
        "score_total":  result["score_total"],
        "score_freq":   result["score_frequencia"],
        "score_div":    result["score_diversitat_carrecs"],
        "score_conn":   result["score_connexio_decisions"],
        "score_valor":  result["score_valor_economic"],
        "total_reunions": result["total_reunions"],
        "total_carrecs":  result["total_carrecs_contactats"],
        "total_connexions": result["total_connexions"],
        "import_total": result["import_total_rebut"],
        "primera_reunio": result["primera_reunio"],
        "ultima_reunio":  result["ultima_reunio"],
        "versio":       result["versio_algorisme"],
    })
