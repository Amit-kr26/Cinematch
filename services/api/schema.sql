-- Movie metadata loaded from MovieLens 1M movies.dat
CREATE TABLE IF NOT EXISTS movies (
    movie_id   INTEGER PRIMARY KEY,
    title      TEXT NOT NULL,
    genres     TEXT NOT NULL,
    year       INTEGER,
    popularity DOUBLE PRECISION DEFAULT 0
);
-- Additive migrations for pre-existing databases
ALTER TABLE movies ADD COLUMN IF NOT EXISTS year       INTEGER;
ALTER TABLE movies ADD COLUMN IF NOT EXISTS popularity DOUBLE PRECISION DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_movies_popularity ON movies(popularity DESC);

-- ALS top-100 candidates per user (baseline, written by bootstrap)
CREATE TABLE IF NOT EXISTS als_candidates (
    user_id    INTEGER PRIMARY KEY,
    candidates JSONB   NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Final top-10 recs per user (updated by Spark streaming, fallback for Redis miss)
CREATE TABLE IF NOT EXISTS user_recs (
    user_id    INTEGER PRIMARY KEY,
    recs       JSONB   NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_recs_updated ON user_recs(updated_at);

-- ALS model training runs — tracks NDCG/Precision across retrains
CREATE TABLE IF NOT EXISTS model_runs (
    id           SERIAL PRIMARY KEY,
    trained_at   TIMESTAMPTZ DEFAULT NOW(),
    rank         INT,
    max_iter     INT,
    reg_param    FLOAT,
    seed         INT,
    ndcg_10      FLOAT,
    precision_10 FLOAT,
    n_users_eval INT,
    is_active    BOOLEAN DEFAULT TRUE
);
