-- ================================================================
-- schema_novector.sql — Schema sense pgvector per a dev/test
-- Usa REAL[] en lloc de vector(768)
-- Aplicar quan pgvector no està instal·lat.
-- Migrar a schema.sql complet quan pgvector estigui disponible.
-- ================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE carrecs (
    id                  SERIAL PRIMARY KEY,
    nom_canonical       TEXT NOT NULL,
    nom_original        TEXT NOT NULL,
    nom_tokens          TEXT[],
    titol               TEXT,
    departament         TEXT,
    departament_codi    TEXT,
    tipologia           TEXT,
    embedding_nom       REAL[],
    creat_at            TIMESTAMPTZ DEFAULT NOW(),
    actualitzat_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (nom_canonical, departament_codi)
);
CREATE INDEX ON carrecs USING gin (nom_tokens);
CREATE INDEX ON carrecs (departament_codi);

CREATE TABLE grups (
    id                      SERIAL PRIMARY KEY,
    codi_registre           TEXT UNIQUE,
    nom                     TEXT NOT NULL,
    nom_canonical           TEXT NOT NULL,
    cif                     TEXT,
    situacio_inscripcio     TEXT,
    ambit_interes           TEXT[],
    objectius               TEXT,
    sector_inferit          TEXT,
    embedding_objectius     REAL[],
    primera_reunio          DATE,
    total_reunions          INTEGER DEFAULT 0,
    creat_at                TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON grups (codi_registre);
CREATE INDEX ON grups (cif);
CREATE INDEX ON grups USING gin (ambit_interes);

CREATE TABLE reunions (
    id                      SERIAL PRIMARY KEY,
    font_id                 TEXT UNIQUE NOT NULL,
    carrec_id               INTEGER REFERENCES carrecs(id),
    grup_id                 INTEGER REFERENCES grups(id),
    data_reunio             DATE NOT NULL,
    departament             TEXT NOT NULL,
    departament_codi        TEXT,
    unitat_organica         TEXT,
    activitat               TEXT,
    tema_original           TEXT NOT NULL,
    tema_normalitzat        TEXT,
    embedding_tema          REAL[],
    nom_grup_original       TEXT,
    nom_registre_grup       TEXT,
    situacio_inscripcio     TEXT,
    creat_at                TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON reunions (carrec_id);
CREATE INDEX ON reunions (grup_id);
CREATE INDEX ON reunions (data_reunio);
CREATE INDEX ON reunions (departament_codi);

CREATE TABLE normativa_dogc (
    id                  SERIAL PRIMARY KEY,
    font_id             TEXT UNIQUE,
    titol               TEXT NOT NULL,
    tipus_norma         TEXT,
    departament         TEXT,
    departament_codi    TEXT,
    data_publicacio     DATE NOT NULL,
    num_dogc            TEXT,
    url_dogc            TEXT,
    resum               TEXT,
    embedding_titol     REAL[],
    embedding_resum     REAL[],
    creat_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON normativa_dogc (data_publicacio);
CREATE INDEX ON normativa_dogc (departament_codi);

CREATE TABLE subvencions_lobby (
    id                  SERIAL PRIMARY KEY,
    font_id             TEXT UNIQUE,
    grup_id             INTEGER REFERENCES grups(id),
    cif_beneficiari     TEXT,
    nom_beneficiari     TEXT,
    import_euros        NUMERIC,
    departament         TEXT,
    departament_codi    TEXT,
    data_concessio      DATE,
    finalitat           TEXT,
    embedding_finalitat REAL[],
    creat_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON subvencions_lobby (grup_id);
CREATE INDEX ON subvencions_lobby (data_concessio);

CREATE TABLE contractes_lobby (
    id                  SERIAL PRIMARY KEY,
    font_id             TEXT UNIQUE,
    grup_id             INTEGER REFERENCES grups(id),
    cif_adjudicatari    TEXT,
    nom_adjudicatari    TEXT,
    import_euros        NUMERIC,
    departament         TEXT,
    departament_codi    TEXT,
    data_adjudicacio    DATE,
    objecte_contracte   TEXT,
    embedding_objecte   REAL[],
    creat_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON contractes_lobby (grup_id);
CREATE INDEX ON contractes_lobby (data_adjudicacio);

CREATE TABLE acords_govern (
    id                  SERIAL PRIMARY KEY,
    font_id             TEXT UNIQUE NOT NULL,
    titol               TEXT NOT NULL,
    departament         TEXT,
    departament_codi    TEXT,
    data_sessio         DATE NOT NULL,
    url_document        TEXT,
    resum               TEXT,
    embedding_titol     REAL[],
    creat_at            TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON acords_govern (data_sessio);
CREATE INDEX ON acords_govern (departament_codi);

CREATE TABLE connexions (
    id                          SERIAL PRIMARY KEY,
    reunio_id                   INTEGER REFERENCES reunions(id) NOT NULL,
    tipus_decisio               TEXT NOT NULL
        CHECK (tipus_decisio IN ('normativa_dogc', 'subvencio', 'contracte', 'acord_govern')),
    decisio_normativa_id        INTEGER REFERENCES normativa_dogc(id),
    decisio_subvencio_id        INTEGER REFERENCES subvencions_lobby(id),
    decisio_contracte_id        INTEGER REFERENCES contractes_lobby(id),
    decisio_acord_govern_id     INTEGER REFERENCES acords_govern(id),
    dies_entre_reunio_decisio   INTEGER NOT NULL,
    similitud_semantica         FLOAT NOT NULL,
    similitud_departament       BOOLEAN NOT NULL,
    similitud_sector            FLOAT,
    connexio_score              FLOAT NOT NULL,
    explicacio_ca               TEXT,
    factors_connexio            TEXT[],
    versio_algorisme            TEXT NOT NULL DEFAULT '1.0.0',
    revisat_manualment          BOOLEAN DEFAULT FALSE,
    es_connexio_valida          BOOLEAN,
    creat_at                    TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT una_decisio CHECK (
        (decisio_normativa_id    IS NOT NULL)::int +
        (decisio_subvencio_id    IS NOT NULL)::int +
        (decisio_contracte_id    IS NOT NULL)::int +
        (decisio_acord_govern_id IS NOT NULL)::int = 1
    )
);
CREATE INDEX ON connexions (reunio_id);
CREATE INDEX ON connexions (connexio_score DESC);
CREATE INDEX ON connexions (dies_entre_reunio_decisio);
CREATE INDEX ON connexions (tipus_decisio);
CREATE UNIQUE INDEX connexions_uniq_normativa  ON connexions (reunio_id, decisio_normativa_id)    WHERE decisio_normativa_id    IS NOT NULL;
CREATE UNIQUE INDEX connexions_uniq_subvencio  ON connexions (reunio_id, decisio_subvencio_id)    WHERE decisio_subvencio_id    IS NOT NULL;
CREATE UNIQUE INDEX connexions_uniq_contracte  ON connexions (reunio_id, decisio_contracte_id)    WHERE decisio_contracte_id    IS NOT NULL;
CREATE UNIQUE INDEX connexions_uniq_acord      ON connexions (reunio_id, decisio_acord_govern_id) WHERE decisio_acord_govern_id IS NOT NULL;

CREATE TABLE lobby_scores (
    id                          SERIAL PRIMARY KEY,
    grup_id                     INTEGER REFERENCES grups(id) UNIQUE,
    score_total                 FLOAT NOT NULL,
    score_frequencia            FLOAT,
    score_diversitat_carrecs    FLOAT,
    score_connexio_decisions    FLOAT,
    score_valor_economic        FLOAT,
    total_reunions              INTEGER DEFAULT 0,
    total_carrecs_contactats    INTEGER DEFAULT 0,
    total_connexions            INTEGER DEFAULT 0,
    import_total_rebut          NUMERIC DEFAULT 0,
    primera_reunio              DATE,
    ultima_reunio               DATE,
    explicacio_ca               TEXT,
    calculat_at                 TIMESTAMPTZ DEFAULT NOW(),
    versio_algorisme            TEXT NOT NULL DEFAULT '1.0.0'
);
CREATE INDEX ON lobby_scores (score_total DESC);

CREATE TABLE alertes (
    id              SERIAL PRIMARY KEY,
    tipus           TEXT NOT NULL
        CHECK (tipus IN ('nova_reunio', 'nova_connexio', 'nou_lobby_actiu', 'score_canvi')),
    grup_id         INTEGER REFERENCES grups(id),
    carrec_id       INTEGER REFERENCES carrecs(id),
    connexio_id     INTEGER REFERENCES connexions(id),
    descripcio      TEXT NOT NULL,
    publicada       BOOLEAN DEFAULT FALSE,
    creat_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON alertes (creat_at DESC);

CREATE MATERIALIZED VIEW top_lobbies AS
SELECT
    g.id, g.nom, g.cif, g.ambit_interes,
    ls.score_total, ls.total_reunions,
    ls.total_carrecs_contactats, ls.total_connexions,
    ls.import_total_rebut, ls.ultima_reunio
FROM grups g
JOIN lobby_scores ls ON ls.grup_id = g.id
ORDER BY ls.score_total DESC;

CREATE MATERIALIZED VIEW top_carrecs_reunions AS
SELECT
    c.id, c.nom_canonical, c.titol, c.departament,
    COUNT(r.id) AS total_reunions,
    COUNT(DISTINCT r.grup_id) AS lobbies_diferents,
    MIN(r.data_reunio) AS primera_reunio,
    MAX(r.data_reunio) AS ultima_reunio
FROM carrecs c
JOIN reunions r ON r.carrec_id = c.id
GROUP BY c.id, c.nom_canonical, c.titol, c.departament
ORDER BY total_reunions DESC;
