"""
Tests de la API FastAPI (sense BD — mock de les dependències).
Verifica que els endpoints responen correctament amb estructura vàlida.
"""

import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Client de test amb BD mockejada via dependency_overrides."""
    from api.main import app
    from db.session import get_db_fastapi

    mock_db = MagicMock()
    mock_db.execute.return_value.fetchall.return_value = []
    mock_db.execute.return_value.fetchone.return_value = None
    mock_db.execute.return_value.scalar.return_value = 0

    def override_get_db():
        yield mock_db

    app.dependency_overrides[get_db_fastapi] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()


class TestEndpointsEstructura:

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_metodologia(self, client):
        r = client.get("/metodologia")
        assert r.status_code == 200
        data = r.json()
        assert "threshold_similitud" in data
        assert "disclamer" in data
        assert data["threshold_similitud"] == 0.72

    def test_stats_endpoint_ok(self, client):
        r = client.get("/stats")
        assert r.status_code == 200

    def test_alertes_endpoint_ok(self, client):
        r = client.get("/alertes")
        assert r.status_code == 200
        assert "items" in r.json()

    def test_ranking_grups(self, client):
        r = client.get("/ranking/grups")
        assert r.status_code == 200
        assert "items" in r.json()

    def test_ranking_carrecs(self, client):
        r = client.get("/ranking/carrecs")
        assert r.status_code == 200

    def test_ranking_connexions(self, client):
        r = client.get("/ranking/connexions")
        assert r.status_code == 200

    def test_llista_grups(self, client):
        r = client.get("/grups")
        assert r.status_code == 200
        data = r.json()
        assert "total" in data
        assert "items" in data
        assert "page" in data

    def test_llista_grups_paginacio(self, client):
        r = client.get("/grups?page=2&limit=10")
        assert r.status_code == 200

    def test_llista_grups_cerca(self, client):
        r = client.get("/grups?q=hospital")
        assert r.status_code == 200

    def test_llista_grups_ordenar_valid(self, client):
        for ordenar in ["score", "reunions", "recent"]:
            r = client.get(f"/grups?ordenar={ordenar}")
            assert r.status_code == 200, f"ordenar={ordenar} ha fallat"

    def test_llista_grups_ordenar_invalid(self, client):
        r = client.get("/grups?ordenar=inexistent")
        assert r.status_code == 422

    def test_grup_no_trobat(self, client):
        r = client.get("/grups/99999")
        assert r.status_code == 404

    def test_llista_carrecs(self, client):
        r = client.get("/carrecs")
        assert r.status_code == 200
        assert "items" in r.json()

    def test_carrec_no_trobat(self, client):
        r = client.get("/carrecs/99999")
        assert r.status_code == 404

    def test_reunions_recents(self, client):
        r = client.get("/reunions/recent")
        assert r.status_code == 200
        assert "items" in r.json()

    def test_reunions_recents_parametre_dies(self, client):
        r = client.get("/reunions/recent?dies=7")
        assert r.status_code == 200

    def test_reunio_no_trobada(self, client):
        r = client.get("/reunions/99999")
        assert r.status_code == 404

    def test_rss_alertes(self, client):
        r = client.get("/rss/alertes.xml")
        assert r.status_code == 200
        assert "application/rss+xml" in r.headers["content-type"]
        assert b"<rss" in r.content


class TestCORS:

    def test_cors_headers(self, client):
        r = client.get("/health", headers={"Origin": "https://lobbyscope.cat"})
        assert r.status_code == 200

    def test_reunions_cerca_sense_pgvector(self, client):
        """Sense PGVECTOR_ENABLED, /cerca retorna 503."""
        import api.routers.reunions as mod
        original = mod._PGVECTOR
        mod._PGVECTOR = False
        try:
            r = client.get("/reunions/cerca?q=salut")
            assert r.status_code == 503
        finally:
            mod._PGVECTOR = original

    def test_reunions_cerca_sense_query(self, client):
        r = client.get("/reunions/cerca")
        assert r.status_code == 422
