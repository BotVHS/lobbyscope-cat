"""
lobbyscope.cat — API REST (FastAPI)

Endpoints:
  GET /grups                    → Llista lobbies amb paginació i cerca
  GET /grups/{id}               → Fitxa completa d'un grup
  GET /grups/{id}/reunions      → Reunions del grup
  GET /grups/{id}/connexions    → Decisions detectades post-reunions
  GET /grups/{id}/score         → Detall del Lobby Influence Score

  GET /carrecs                  → Llista alts càrrecs
  GET /carrecs/{id}             → Fitxa d'un càrrec
  GET /carrecs/{id}/reunions    → Agenda completa del càrrec

  GET /reunions/{id}            → Detall reunió + connexions
  GET /reunions/recent          → Reunions dels darrers N dies

  GET /ranking/grups            → Top lobbies per score
  GET /ranking/carrecs          → Top càrrecs per nombre de reunions
  GET /ranking/connexions       → Connexions d'alt score

  GET /stats                    → Estadístiques globals
  GET /alertes                  → Novetats setmana
  GET /metodologia              → Document de metodologia

  Feed RSS:
  GET /rss/alertes.xml
  GET /rss/grups/{id}.xml
"""

import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

from api.routers import grups, carrecs, reunions, ranking, stats, rss

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="lobbyscope.cat API",
    description="Tracker de lobbisme a la Generalitat de Catalunya",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Restringir al domini de producció quan s'hi desplega
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(grups.router,    prefix="/grups",    tags=["grups"])
app.include_router(carrecs.router,  prefix="/carrecs",  tags=["carrecs"])
app.include_router(reunions.router, prefix="/reunions", tags=["reunions"])
app.include_router(ranking.router,  prefix="/ranking",  tags=["ranking"])
app.include_router(stats.router,    tags=["stats"])
app.include_router(rss.router,      prefix="/rss",      tags=["rss"])


@app.get("/metodologia", tags=["info"])
def metodologia():
    return {
        "versio_algorisme": "1.0.0",
        "threshold_similitud": 0.72,
        "finestra_dies_abans": 30,
        "finestra_dies_despres": 180,
        "model_embeddings": os.getenv("EMBEDDING_MODEL", "sentence-transformers/LaBSE"),
        "dimensions_vector": int(os.getenv("EMBEDDING_DIMS", "768")),
        "scoring": {
            "similitud_semantica": "0-50 punts",
            "proximitat_temporal": "0-30 punts",
            "coincidencia_departament": "0-15 punts",
            "import_economic": "0-5 punts",
        },
        "disclamer": (
            "Les connexions mostrades són correlacions estadístiques entre "
            "temes de reunions i decisions posteriors. No impliquen cap relació "
            "causal ni cap irregularitat. El portal és una eina de transparència "
            "i no d'acusació."
        ),
    }


@app.get("/health", tags=["info"])
def health():
    return {"status": "ok"}
