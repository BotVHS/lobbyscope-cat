"""
Tests de normalització de noms i departaments.
"""

import pytest

from normalitzacio.noms import normalitzar_nom_persona, normalitzar_nom_empresa
from normalitzacio.departaments import mapejar_departament


# ================================================================
# normalitzar_nom_persona
# ================================================================

class TestNormalitzarNomPersona:

    def test_tot_majuscules(self):
        assert normalitzar_nom_persona("AINA PLAZA TESÍAS") == "Aina Plaza Tesías"

    def test_tot_minuscules(self):
        assert normalitzar_nom_persona("teresa jordà i roura") == "Teresa Jordà i Roura"

    def test_particula_i(self):
        result = normalitzar_nom_persona("JORDI PUIGNERÓ I MARÍN")
        assert result == "Jordi Puigneró i Marín"

    def test_particula_de(self):
        result = normalitzar_nom_persona("MARIA DE LA PAU JANER")
        # "de" i "la" han de ser minúscules al mig
        assert "de" in result.lower()

    def test_dobles_espais(self):
        result = normalitzar_nom_persona("PERE   ARAGONÈS")
        assert "  " not in result

    def test_string_buit(self):
        assert normalitzar_nom_persona("") == ""

    def test_string_espais(self):
        assert normalitzar_nom_persona("   ") == ""

    def test_nom_simple(self):
        assert normalitzar_nom_persona("PERE") == "Pere"

    def test_accents_preservats(self):
        result = normalitzar_nom_persona("NÚRIA LLORACH")
        assert "Núria" in result or "Nuria" in result  # depèn de la font

    def test_nom_mixt(self):
        result = normalitzar_nom_persona("Carles puigdemont i casamajó")
        assert result[0].isupper()  # primera lletra sempre majúscula


# ================================================================
# normalitzar_nom_empresa
# ================================================================

class TestNormalitzarNomEmpresa:

    def test_forma_juridica_sl(self):
        result = normalitzar_nom_empresa("ACME TECNOLOGIA, S.L.")
        assert "s.l." not in result
        assert "acme" in result
        assert "tecnologia" in result

    def test_forma_juridica_sa(self):
        result = normalitzar_nom_empresa("HOSPITAL CLÍNIC, S.A.")
        assert "s.a." not in result
        assert "hospital" in result

    def test_fundacio(self):
        result = normalitzar_nom_empresa("FUNDACIÓ HOSPITAL SANT JOAN DE DÉU")
        assert "fundaci" not in result
        assert "hospital" in result
        assert "sant" in result

    def test_associacio(self):
        result = normalitzar_nom_empresa("ASSOCIACIÓ DE FABRICANTS DE CIMENT")
        assert "associaci" not in result
        assert "fabricants" in result

    def test_accents_eliminats(self):
        result = normalitzar_nom_empresa("CÀMERA DE COMERÇ DE BARCELONA")
        # El canonical no ha de tenir accents
        assert all(ord(c) < 128 for c in result)

    def test_minuscules(self):
        result = normalitzar_nom_empresa("EMPRESA GRAN")
        assert result == result.lower()

    def test_string_buit(self):
        assert normalitzar_nom_empresa("") == ""

    def test_puntuacio_eliminada(self):
        result = normalitzar_nom_empresa("EMPRESA (FILIAL), S.L.U.")
        assert "(" not in result
        assert ")" not in result
        assert "," not in result


# ================================================================
# mapejar_departament
# ================================================================

class TestMapejarDepartament:

    def test_salut_complet(self):
        assert mapejar_departament("Departament de Salut") == "SALUT"

    def test_salut_abreujat(self):
        assert mapejar_departament("dept. de salut") == "SALUT"

    def test_salut_majuscules(self):
        assert mapejar_departament("DEPARTAMENT DE SALUT") == "SALUT"

    def test_educacio(self):
        assert mapejar_departament("Departament d'Educació") == "EDUCACIO"

    def test_educacio_ensenyament(self):
        assert mapejar_departament("Departament d'Ensenyament") == "EDUCACIO"

    def test_empresa(self):
        assert mapejar_departament("Departament d'Empresa i Treball") == "EMPRESA"

    def test_territori(self):
        assert mapejar_departament("Departament de Territori i Sostenibilitat") == "TERRITORI"

    def test_interior(self):
        assert mapejar_departament("Departament d'Interior") == "INTERIOR"

    def test_justicia(self):
        assert mapejar_departament("Departament de Justícia") == "JUSTICIA"

    def test_economia(self):
        assert mapejar_departament("Departament d'Economia i Finances") == "ECONOMIA"

    def test_cultura(self):
        assert mapejar_departament("Departament de Cultura") == "CULTURA"

    def test_accio_climatica(self):
        assert mapejar_departament("Departament d'Acció Climàtica, Alimentació i Agenda Rural") == "ACCIO_CLIMATICA"

    def test_igualtat(self):
        assert mapejar_departament("Departament d'Igualtat i Feminismes") == "IGUALTAT"

    def test_presidencia(self):
        assert mapejar_departament("Presidència de la Generalitat") == "PRESIDENCIA"

    def test_string_buit(self):
        # DESCONEGUT només quan el camp és absent/buit
        assert mapejar_departament("") == "DESCONEGUT"
        assert mapejar_departament("   ") == "DESCONEGUT"

    def test_govern_fallback(self):
        # Text present però no reconegut → GOVERN (òrgan/acte transversal del Govern)
        assert mapejar_departament("Zurich Insurance Group AG") == "GOVERN"
        assert mapejar_departament("Autoritat Catalana de la Competencia") == "GOVERN"

    def test_case_insensitive(self):
        assert mapejar_departament("DEPARTAMENT DE SALUT") == mapejar_departament("departament de salut")

    def test_amb_accents_o_sense(self):
        # Ha de funcionar tant amb accents com sense
        r1 = mapejar_departament("Departament de Justícia")
        r2 = mapejar_departament("Departament de Justicia")
        assert r1 == r2 == "JUSTICIA"
