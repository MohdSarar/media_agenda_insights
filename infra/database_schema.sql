CREATE TABLE IF NOT EXISTS articles_raw (
    id              SERIAL PRIMARY KEY,
    source          TEXT NOT NULL,      -- cnews, bfmtv, franceinfo, tf1info, etc.
    media_type      TEXT NOT NULL,      -- 'tv' ou 'press'
    feed_name       TEXT NOT NULL,      -- ex: 'actu_direct', 'fil_info'
    title           TEXT NOT NULL,
    summary         TEXT,
    url             TEXT NOT NULL UNIQUE,
    published_at    TIMESTAMP,          -- date fournie par le flux
    inserted_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    raw_content     TEXT                -- si plus tard tu veux récupérer le HTML complet
);



CREATE TABLE IF NOT EXISTS articles_clean (
    article_id   INTEGER PRIMARY KEY REFERENCES articles_raw(id) ON DELETE CASCADE,
    cleaned_text TEXT NOT NULL,
    tokens       TEXT[] NULL,
    lemmas       TEXT[] NULL,
    lang         TEXT DEFAULT 'fr',
    entities     JSONB NULL             -- NER (PERSON, ORG, LOC, etc.)
);

CREATE TABLE IF NOT EXISTS keywords_daily (
    id           SERIAL PRIMARY KEY,
    date         DATE NOT NULL,
    source       TEXT NOT NULL,         -- ou 'ALL' pour global
    media_type   TEXT NOT NULL,         -- 'tv', 'press', 'social', ou 'ALL'
    word         TEXT NOT NULL,
    count        INTEGER NOT NULL,
    rank         INTEGER NOT NULL,      -- 1 à 10
    UNIQUE (date, source, media_type, word)
);


CREATE TABLE IF NOT EXISTS topics_daily (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    source          TEXT NOT NULL,          -- 'cnews', 'bfmtv', etc. ou 'ALL'
    media_type      TEXT NOT NULL,
    topic_id        INTEGER NOT NULL,       -- id interne du cluster/sujet
    topic_label     TEXT,                   -- label interprété humainement
    articles_count  INTEGER NOT NULL,
    keywords        TEXT[] NULL,            -- mots représentatifs du sujet
    UNIQUE (date, source, media_type, topic_id)
);


CREATE TABLE IF NOT EXISTS narratives_comparison (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    topic_id        INTEGER NOT NULL,
    source_a        TEXT NOT NULL,
    source_b        TEXT NOT NULL,
    similarity      REAL NOT NULL,         -- ex : cosine similarity
    details         JSONB NULL             -- ex: exemples d’articles comparés
);


CREATE TABLE IF NOT EXISTS media_bias_scores (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    source          TEXT NOT NULL,
    theme           TEXT NOT NULL,         -- ex : 'immigration', 'sécurité', 'économie'
    bias_score      REAL NOT NULL,         -- indicateur maison
    methodology     TEXT NULL,             -- description courte
    details         JSONB NULL
);


CREATE TABLE IF NOT EXISTS topic_lifetime (
    id                SERIAL PRIMARY KEY,
    topic_id          INTEGER NOT NULL,
    topic_label       TEXT,
    first_seen_date   DATE NOT NULL,
    last_seen_date    DATE NOT NULL,
    peak_date         DATE,
    total_mentions    INTEGER NOT NULL,
    sources_covered   TEXT[] NULL          -- ex: ['cnews','bfmtv','franceinfo']
);



CREATE TABLE IF NOT EXISTS spikes (
    id              SERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    topic_id        INTEGER NOT NULL,
    source          TEXT NOT NULL,         -- ou 'ALL'
    spike_score     REAL NOT NULL,         -- mesure de l’anomalie
    baseline_window INTEGER NOT NULL,      -- nb de jours utilisés comme baseline
    details         JSONB NULL
);



CREATE TABLE IF NOT EXISTS narratives_clusters (
    id            SERIAL PRIMARY KEY,
    cluster_id    INTEGER NOT NULL UNIQUE,  -- id du cluster (modèle)
    label         TEXT,                     -- label textuel du narratif
    top_keywords  TEXT[] NOT NULL,          -- mots/lemmes représentatifs
    size          INTEGER NOT NULL,         -- nombre d’articles dans le cluster
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS narratives_assignments (
    id          SERIAL PRIMARY KEY,
    cluster_id  INTEGER NOT NULL,
    article_id  INTEGER NOT NULL REFERENCES articles_raw(id) ON DELETE CASCADE,
    distance    REAL,                       -- distance dans l’espace d’embedding (optionnel)
    assigned_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index utiles pour les performances
CREATE INDEX IF NOT EXISTS idx_narratives_assignments_cluster
    ON narratives_assignments (cluster_id);

CREATE INDEX IF NOT EXISTS idx_narratives_assignments_article
    ON narratives_assignments (article_id);

CREATE INDEX IF NOT EXISTS idx_keywords_daily_date_source_media
    ON keywords_daily (date, source, media_type);

CREATE INDEX IF NOT EXISTS idx_topics_daily_date_source_media
    ON topics_daily (date, source, media_type);

CREATE INDEX IF NOT EXISTS idx_articles_raw_published_source
    ON articles_raw (published_at, source, media_type);
