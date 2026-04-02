-- ================================================================
-- Índexs vectorials ivfflat — executar DESPRÉS de la primera ingesta
--
-- Requisit: >10.000 registres a cada taula per a un calibratge correcte.
-- Com executar:
--   psql $DATABASE_URL -f db/create_vector_indexes.sql
-- ================================================================

CREATE INDEX IF NOT EXISTS reunions_embedding_tema_idx
    ON reunions USING ivfflat (embedding_tema vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS normativa_dogc_embedding_titol_idx
    ON normativa_dogc USING ivfflat (embedding_titol vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS normativa_dogc_embedding_resum_idx
    ON normativa_dogc USING ivfflat (embedding_resum vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS grups_embedding_objectius_idx
    ON grups USING ivfflat (embedding_objectius vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS carrecs_embedding_nom_idx
    ON carrecs USING ivfflat (embedding_nom vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS subvencions_embedding_finalitat_idx
    ON subvencions_lobby USING ivfflat (embedding_finalitat vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS contractes_embedding_objecte_idx
    ON contractes_lobby USING ivfflat (embedding_objecte vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX IF NOT EXISTS acords_govern_embedding_titol_idx
    ON acords_govern USING ivfflat (embedding_titol vector_cosine_ops)
    WITH (lists = 100);
