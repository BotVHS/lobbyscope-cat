"""
Gestió de sessions de base de dades.

Usar get_db() com a context manager o generator (FastAPI):

    # Script standalone:
    with get_db() as db:
        db.query(Grup).all()

    # FastAPI dependency:
    def my_endpoint(db: Session = Depends(get_db)):
        ...
"""

import os
from contextlib import contextmanager
from typing import Generator

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,       # detecta connexions trencades
    pool_size=5,
    max_overflow=10,
    echo=False,               # True per a debug SQL
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
)


@contextmanager
def get_db() -> Generator[Session, None, None]:
    """Context manager per a scripts i tests."""
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_db_fastapi() -> Generator[Session, None, None]:
    """Generator per a FastAPI Depends()."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
