DROP TABLE IF EXISTS chain;

DROP TYPE IF EXISTS bigram;
DROP TYPE IF EXISTS seq_unigram;

CREATE TYPE bigram AS (first text, second text);
CREATE TYPE seq_unigram AS (seq_num integer, unigram text);

CREATE UNLOGGED TABLE chain (
    bigram        bigram NOT NULL,
    topic         bigram NOT NULL,

    next_unigrams seq_unigram[] NOT NULL
) WITH (autovacuum_enabled=false, oids=false);
