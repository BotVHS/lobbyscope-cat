"""
Tests del mòdul de processament (sense BD ni API).
"""

import pytest
from processament.detector_connexions import calcular_score_connexio
from processament.scores import (
    _score_frequencia,
    _score_diversitat,
    _score_connexions,
    _score_valor_economic,
)


# ================================================================
# calcular_score_connexio
# ================================================================

class TestScoreConnexio:

    def test_alta_similitud_dept_match_recent(self):
        """Connexió ideal: tema molt similar, mateix dept, recent."""
        score = calcular_score_connexio(similitud=0.89, dies=15, dept_match=True)
        assert score >= 75, f"Score massa baix per a connexió ideal: {score}"

    def test_baixa_similitud_dept_diferent_lluny(self):
        """Similitud baixa + dept diferent + lluny → score baix.
        LaBSE real: 0.18 és una similitud molt baixa (sostre real ~0.48)."""
        score = calcular_score_connexio(similitud=0.18, dies=160, dept_match=False)
        assert score < 40, f"Score massa alt: {score}"

    def test_penalitzacio_llunyania_temporal(self):
        """Decisions molt posteriors han de puntuar menys."""
        score_recent = calcular_score_connexio(0.82, dies=10, dept_match=True)
        score_lluny  = calcular_score_connexio(0.82, dies=170, dept_match=True)
        assert score_recent > score_lluny, (
            f"El score recent ({score_recent}) ha de ser > llunyà ({score_lluny})"
        )

    def test_bonus_dept_match(self):
        """Mateix departament ha de puntuar més que departament diferent."""
        score_same = calcular_score_connexio(0.75, dies=30, dept_match=True)
        score_diff = calcular_score_connexio(0.75, dies=30, dept_match=False)
        assert score_same > score_diff

    def test_bonus_import_economic(self):
        """Subvenció d'1M€ ha de puntuar més que sense import."""
        score_import = calcular_score_connexio(0.75, dies=30, dept_match=True, import_euros=1_500_000)
        score_sense  = calcular_score_connexio(0.75, dies=30, dept_match=True, import_euros=0)
        assert score_import > score_sense

    def test_score_maxim_100(self):
        """El score no pot superar 100."""
        score = calcular_score_connexio(1.0, dies=0, dept_match=True, import_euros=10_000_000)
        assert score <= 100.0

    def test_score_minim_positiu(self):
        """El score ha de ser sempre positiu."""
        score = calcular_score_connexio(0.0, dies=500, dept_match=False)
        assert score >= 0.0

    def test_decisions_anteriors_a_reunio(self):
        """Decisions lleugerament anteriors a la reunió (dies negatiu) tenen sentit."""
        score = calcular_score_connexio(0.80, dies=-10, dept_match=True)
        assert score > 0

    def test_threshold_limit(self):
        """Similitud exactament al threshold ha de generar score usable."""
        score = calcular_score_connexio(0.72, dies=45, dept_match=True)
        assert score >= 30


# ================================================================
# Components del Lobby Influence Score
# ================================================================

class TestLobbyScoreComponents:

    def test_frequencia_alta(self):
        assert _score_frequencia(50) == 25.0
        assert _score_frequencia(100) == 25.0

    def test_frequencia_baixa(self):
        assert _score_frequencia(1) == 2.0
        assert _score_frequencia(3) < _score_frequencia(10)

    def test_diversitat_monotona(self):
        """Més diversitat de càrrecs i departaments → més score."""
        s_baixa = _score_diversitat(carrecs_diferents=1,  departaments_diferents=1, total_reunions=20)
        s_alta  = _score_diversitat(carrecs_diferents=15, departaments_diferents=4, total_reunions=20)
        assert s_alta > s_baixa

    def test_diversitat_maxima_25(self):
        assert _score_diversitat(carrecs_diferents=50, departaments_diferents=10, total_reunions=50) <= 25.0

    def test_connexions_cap(self):
        assert _score_connexions(0, 10) == 0.0

    def test_connexions_alta_taxa(self):
        score = _score_connexions(total_connexions=8, total_reunions=10)
        assert score >= 25.0

    def test_valor_economic_escales(self):
        assert _score_valor_economic(0) == 0.0
        assert _score_valor_economic(50_000) < _score_valor_economic(500_000)
        assert _score_valor_economic(50_000_000) == 20.0

    def test_zero_reunions_no_crash(self):
        """No ha de petar si total_reunions == 0."""
        assert _score_diversitat(carrecs_diferents=0, departaments_diferents=0, total_reunions=0) == 0.0
        assert _score_connexions(0, 0) == 0.0


# ================================================================
# Classificador: imports i funció per defecte (sense API)
# ================================================================

class TestClassificador:

    def test_import_ok(self):
        """El mòdul ha de ser importable sense clau API."""
        from processament.classificador import DISCLAMER_CA, _resultat_per_defecte
        assert len(DISCLAMER_CA) > 10

    def test_resultat_per_defecte(self):
        from processament.classificador import _resultat_per_defecte
        r = _resultat_per_defecte()
        assert "narrativa_ca" in r
        assert "connexio_tematica" in r
        assert r["recomanacio_revisio"] is True

    def test_disclamer_present_en_resultat_defecte(self):
        from processament.classificador import _resultat_per_defecte, DISCLAMER_CA
        r = _resultat_per_defecte()
        assert DISCLAMER_CA in r["narrativa_ca"]
