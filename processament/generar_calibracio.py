"""
Genera un CSV de calibració estratificat per validar manualment la precisió del detector.

Estratègia de mostreig:
  - 40 parelles amb score >= 70  (zona de qualitat, CONNEXIO_SCORE_MIN)
  - 40 parelles amb score 50-69  (zona grisa)
  - 20 parelles amb score 30-49  (zona baixa, esperem molts FPs)
  Total: 100 parelles aleatòries estratificades per capes de score.

Ús:
  python -m processament.generar_calibracio
  # Escriu C:/Users/<user>/Downloads/calibracio_connexions_v<X>.csv
"""

import csv
import os
import random
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent.parent))
from db.session import SessionLocal
from processament.detector_connexions import VERSIO_ALGORISME

OUTPUT_DIR = Path(os.environ.get("USERPROFILE", Path.home())) / "Downloads"
OUTPUT_FILE = OUTPUT_DIR / f"calibracio_connexions_v{VERSIO_ALGORISME.replace('.', '')}.csv"

QUERY = text("""
    SELECT
        c.id,
        c.connexio_score                          AS score,
        c.similitud_semantica                     AS sim,
        c.similitud_departament                   AS dept_match,
        c.dies_entre_reunio_decisio               AS dies,
        COALESCE(g.nom, '—')                      AS grup,
        r.departament_codi                        AS dept_reunio,
        r.data_reunio,
        r.tema_normalitzat                        AS tema_reunio,
        n.departament_codi                        AS dept_norma,
        n.data_publicacio                         AS data_norma,
        LEFT(n.titol, 120)                        AS titol_norma
    FROM connexions c
    JOIN reunions r        ON r.id = c.reunio_id
    JOIN normativa_dogc n  ON n.id = c.decisio_normativa_id
    LEFT JOIN grups g      ON g.id = r.grup_id
    WHERE c.versio_algorisme = :versio
      AND c.tipus_decisio = 'normativa_dogc'
      AND c.dies_entre_reunio_decisio >= 0
    ORDER BY RANDOM()
""")


def _mostreig_estratificat(db) -> list[dict]:
    """Mostra 40+40+20 parelles des de tres franges de score."""
    capes = [
        ("alta",  "c.connexio_score >= 70",              40),
        ("mitja", "c.connexio_score >= 50 AND c.connexio_score < 70", 40),
        ("baixa", "c.connexio_score >= 30 AND c.connexio_score < 50", 20),
    ]
    resultat = []
    for nom, where, n in capes:
        rows = db.execute(text(f"""
            SELECT
                c.id,
                c.connexio_score                          AS score,
                c.similitud_semantica                     AS sim,
                c.similitud_departament                   AS dept_match,
                c.dies_entre_reunio_decisio               AS dies,
                COALESCE(g.nom, '—')                      AS grup,
                r.departament_codi                        AS dept_reunio,
                r.data_reunio::text                       AS data_reunio,
                r.tema_normalitzat                        AS tema_reunio,
                n.departament_codi                        AS dept_norma,
                n.data_publicacio::text                   AS data_norma,
                LEFT(n.titol, 120)                        AS titol_norma
            FROM connexions c
            JOIN reunions r        ON r.id = c.reunio_id
            JOIN normativa_dogc n  ON n.id = c.decisio_normativa_id
            LEFT JOIN grups g      ON g.id = r.grup_id
            WHERE c.versio_algorisme = :versio
              AND c.tipus_decisio = 'normativa_dogc'
              AND c.dies_entre_reunio_decisio >= 0
              AND {where}
            ORDER BY RANDOM()
            LIMIT {n * 3}
        """), {"versio": VERSIO_ALGORISME}).fetchall()

        # Desduplicar per tema_reunio (evitar múltiples normes del mateix meeting)
        seen_reunio = set()
        seleccionats = []
        for r in rows:
            if r.tema_reunio not in seen_reunio:
                seen_reunio.add(r.tema_reunio)
                seleccionats.append(r)
            if len(seleccionats) == n:
                break

        print(f"  Capa {nom}: {len(seleccionats)}/{n} parelles")
        resultat.extend(seleccionats)

    random.shuffle(resultat)
    return resultat


def main():
    db = SessionLocal()
    try:
        n_total = db.execute(text(
            "SELECT COUNT(*) FROM connexions WHERE versio_algorisme = :v",
        ), {"v": VERSIO_ALGORISME}).scalar()
        print(f"Connexions v{VERSIO_ALGORISME}: {n_total:,}")

        print("Mostrejant 100 parelles estratificades...")
        parelles = _mostreig_estratificat(db)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow([
                "id", "score", "sim", "dept_match", "dies",
                "grup", "dept_reunio", "data_reunio", "tema_reunio",
                "dept_norma", "data_norma", "titol_norma",
                "validacio_010", "explicacio",
            ])
            for p in parelles:
                writer.writerow([
                    p.id, p.score, p.sim, p.dept_match, p.dies,
                    p.grup, p.dept_reunio, p.data_reunio, p.tema_reunio,
                    p.dept_norma, p.data_norma, p.titol_norma,
                    "",  # validació: 0-10 (0=sense relació, 10=relació directa quasi segura)
                    "",  # explicació opcional
                ])

        print(f"\nCSV generat: {OUTPUT_FILE}")
        print("Omple la columna 'validacio_010' amb un valor de 0 a 10:")
        print("  0  = sense cap relació")
        print("  5  = possible relació però incerta")
        print("  10 = relació directa quasi segura")

    finally:
        db.close()


if __name__ == "__main__":
    main()
