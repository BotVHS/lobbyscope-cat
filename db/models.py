"""
SQLAlchemy 2.x ORM models per a lobbyscope.cat.

Mapeja exactament les taules de db/schema.sql.
Usa pgvector.sqlalchemy.Vector per a les columnes d'embedding (768 dims).
"""

from datetime import date, datetime
from typing import Optional

import os
from sqlalchemy import (
    Boolean, CheckConstraint, Date, Float, ForeignKey, Integer,
    Numeric, String, Text, ARRAY, UniqueConstraint, TIMESTAMP,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# TIMESTAMPTZ = TIMESTAMP with timezone (compatible PostgreSQL + SQLAlchemy 2.x)
TIMESTAMPTZ = TIMESTAMP(timezone=True)


EMBEDDING_DIMS = 768

# Usar Vector(768) si PGVECTOR_ENABLED=true a .env, sino ARRAY(Float)
# Canviar a true quan pgvector estigui instal·lat a PostgreSQL.
if os.getenv("PGVECTOR_ENABLED", "false").lower() == "true":
    from pgvector.sqlalchemy import Vector
    _EmbeddingType = Vector(EMBEDDING_DIMS)
else:
    _EmbeddingType = ARRAY(Float)  # type: ignore


class Base(DeclarativeBase):
    pass


class Carrec(Base):
    __tablename__ = "carrecs"
    __table_args__ = (
        UniqueConstraint("nom_canonical", "departament_codi"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nom_canonical: Mapped[str] = mapped_column(Text, nullable=False)
    nom_original: Mapped[str] = mapped_column(Text, nullable=False)
    nom_tokens: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    titol: Mapped[Optional[str]] = mapped_column(Text)
    departament: Mapped[Optional[str]] = mapped_column(Text)
    departament_codi: Mapped[Optional[str]] = mapped_column(Text)
    tipologia: Mapped[Optional[str]] = mapped_column(Text)
    embedding_nom: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)
    actualitzat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)

    reunions: Mapped[list["Reunio"]] = relationship(back_populates="carrec")

    def __repr__(self) -> str:
        return f"<Carrec {self.nom_canonical} ({self.departament_codi})>"


class Grup(Base):
    __tablename__ = "grups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    codi_registre: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    nom: Mapped[str] = mapped_column(Text, nullable=False)
    nom_canonical: Mapped[str] = mapped_column(Text, nullable=False)
    cif: Mapped[Optional[str]] = mapped_column(Text)
    situacio_inscripcio: Mapped[Optional[str]] = mapped_column(Text)
    ambit_interes: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    objectius: Mapped[Optional[str]] = mapped_column(Text)
    sector_inferit: Mapped[Optional[str]] = mapped_column(Text)
    embedding_objectius: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    primera_reunio: Mapped[Optional[date]] = mapped_column(Date)
    total_reunions: Mapped[int] = mapped_column(Integer, default=0)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)

    reunions: Mapped[list["Reunio"]] = relationship(back_populates="grup")
    score: Mapped[Optional["LobbyScore"]] = relationship(back_populates="grup", uselist=False)

    def __repr__(self) -> str:
        return f"<Grup {self.nom} ({self.codi_registre})>"


class Reunio(Base):
    __tablename__ = "reunions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    font_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    carrec_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("carrecs.id"))
    grup_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("grups.id"))
    data_reunio: Mapped[date] = mapped_column(Date, nullable=False)
    departament: Mapped[str] = mapped_column(Text, nullable=False)
    departament_codi: Mapped[Optional[str]] = mapped_column(Text)
    unitat_organica: Mapped[Optional[str]] = mapped_column(Text)
    activitat: Mapped[Optional[str]] = mapped_column(Text)
    tema_original: Mapped[str] = mapped_column(Text, nullable=False)
    tema_normalitzat: Mapped[Optional[str]] = mapped_column(Text)
    embedding_tema: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    nom_grup_original: Mapped[Optional[str]] = mapped_column(Text)
    nom_registre_grup: Mapped[Optional[str]] = mapped_column(Text)
    situacio_inscripcio: Mapped[Optional[str]] = mapped_column(Text)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)

    carrec: Mapped[Optional["Carrec"]] = relationship(back_populates="reunions")
    grup: Mapped[Optional["Grup"]] = relationship(back_populates="reunions")
    connexions: Mapped[list["Connexio"]] = relationship(back_populates="reunio")

    def __repr__(self) -> str:
        return f"<Reunio {self.font_id} {self.data_reunio}>"


class NormativaDogc(Base):
    __tablename__ = "normativa_dogc"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    font_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    titol: Mapped[str] = mapped_column(Text, nullable=False)
    tipus_norma: Mapped[Optional[str]] = mapped_column(Text)
    departament: Mapped[Optional[str]] = mapped_column(Text)
    departament_codi: Mapped[Optional[str]] = mapped_column(Text)
    data_publicacio: Mapped[date] = mapped_column(Date, nullable=False)
    num_dogc: Mapped[Optional[str]] = mapped_column(Text)
    url_dogc: Mapped[Optional[str]] = mapped_column(Text)
    resum: Mapped[Optional[str]] = mapped_column(Text)
    embedding_titol: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    embedding_resum: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<NormativaDogc {self.data_publicacio} {self.titol[:50]}>"


class SubvencioLobby(Base):
    __tablename__ = "subvencions_lobby"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    font_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    grup_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("grups.id"))
    cif_beneficiari: Mapped[Optional[str]] = mapped_column(Text)
    nom_beneficiari: Mapped[Optional[str]] = mapped_column(Text)
    import_euros: Mapped[Optional[float]] = mapped_column(Numeric)
    departament: Mapped[Optional[str]] = mapped_column(Text)
    departament_codi: Mapped[Optional[str]] = mapped_column(Text)
    data_concessio: Mapped[Optional[date]] = mapped_column(Date)
    finalitat: Mapped[Optional[str]] = mapped_column(Text)
    embedding_finalitat: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)


class ContracteLobby(Base):
    __tablename__ = "contractes_lobby"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    font_id: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    grup_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("grups.id"))
    cif_adjudicatari: Mapped[Optional[str]] = mapped_column(Text)
    nom_adjudicatari: Mapped[Optional[str]] = mapped_column(Text)
    import_euros: Mapped[Optional[float]] = mapped_column(Numeric)
    departament: Mapped[Optional[str]] = mapped_column(Text)
    departament_codi: Mapped[Optional[str]] = mapped_column(Text)
    data_adjudicacio: Mapped[Optional[date]] = mapped_column(Date)
    objecte_contracte: Mapped[Optional[str]] = mapped_column(Text)
    embedding_objecte: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)


class AcordGovern(Base):
    __tablename__ = "acords_govern"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    font_id: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    titol: Mapped[str] = mapped_column(Text, nullable=False)
    departament: Mapped[Optional[str]] = mapped_column(Text)
    departament_codi: Mapped[Optional[str]] = mapped_column(Text)
    data_sessio: Mapped[date] = mapped_column(Date, nullable=False)
    url_document: Mapped[Optional[str]] = mapped_column(Text)
    resum: Mapped[Optional[str]] = mapped_column(Text)
    embedding_titol: Mapped[Optional[list]] = mapped_column(_EmbeddingType)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)

    def __repr__(self) -> str:
        return f"<AcordGovern {self.data_sessio} {self.titol[:50]}>"


class Connexio(Base):
    __tablename__ = "connexions"
    __table_args__ = (
        CheckConstraint(
            "(decisio_normativa_id IS NOT NULL)::int + "
            "(decisio_subvencio_id IS NOT NULL)::int + "
            "(decisio_contracte_id IS NOT NULL)::int + "
            "(decisio_acord_govern_id IS NOT NULL)::int = 1",
            name="una_decisio",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    reunio_id: Mapped[int] = mapped_column(Integer, ForeignKey("reunions.id"), nullable=False)
    tipus_decisio: Mapped[str] = mapped_column(Text, nullable=False)
    decisio_normativa_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("normativa_dogc.id"))
    decisio_subvencio_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("subvencions_lobby.id"))
    decisio_contracte_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("contractes_lobby.id"))
    decisio_acord_govern_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("acords_govern.id"))
    dies_entre_reunio_decisio: Mapped[int] = mapped_column(Integer, nullable=False)
    similitud_semantica: Mapped[float] = mapped_column(Float, nullable=False)
    similitud_departament: Mapped[bool] = mapped_column(Boolean, nullable=False)
    similitud_sector: Mapped[Optional[float]] = mapped_column(Float)
    connexio_score: Mapped[float] = mapped_column(Float, nullable=False)
    explicacio_ca: Mapped[Optional[str]] = mapped_column(Text)
    factors_connexio: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    versio_algorisme: Mapped[str] = mapped_column(Text, default="1.0.0")
    revisat_manualment: Mapped[bool] = mapped_column(Boolean, default=False)
    es_connexio_valida: Mapped[Optional[bool]] = mapped_column(Boolean)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)

    reunio: Mapped["Reunio"] = relationship(back_populates="connexions")

    def __repr__(self) -> str:
        return f"<Connexio reunio={self.reunio_id} score={self.connexio_score}>"


class LobbyScore(Base):
    __tablename__ = "lobby_scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    grup_id: Mapped[int] = mapped_column(Integer, ForeignKey("grups.id"), unique=True)
    score_total: Mapped[float] = mapped_column(Float, nullable=False)
    score_frequencia: Mapped[Optional[float]] = mapped_column(Float)
    score_diversitat_carrecs: Mapped[Optional[float]] = mapped_column(Float)
    score_connexio_decisions: Mapped[Optional[float]] = mapped_column(Float)
    score_valor_economic: Mapped[Optional[float]] = mapped_column(Float)
    total_reunions: Mapped[int] = mapped_column(Integer, default=0)
    total_carrecs_contactats: Mapped[int] = mapped_column(Integer, default=0)
    total_connexions: Mapped[int] = mapped_column(Integer, default=0)
    import_total_rebut: Mapped[float] = mapped_column(Numeric, default=0)
    primera_reunio: Mapped[Optional[date]] = mapped_column(Date)
    ultima_reunio: Mapped[Optional[date]] = mapped_column(Date)
    explicacio_ca: Mapped[Optional[str]] = mapped_column(Text)
    calculat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)
    versio_algorisme: Mapped[str] = mapped_column(Text, default="1.0.0")

    grup: Mapped["Grup"] = relationship(back_populates="score")


class Alerta(Base):
    __tablename__ = "alertes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tipus: Mapped[str] = mapped_column(Text, nullable=False)
    grup_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("grups.id"))
    carrec_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("carrecs.id"))
    connexio_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("connexions.id"))
    descripcio: Mapped[str] = mapped_column(Text, nullable=False)
    publicada: Mapped[bool] = mapped_column(Boolean, default=False)
    creat_at: Mapped[datetime] = mapped_column(TIMESTAMPTZ, default=datetime.utcnow)
