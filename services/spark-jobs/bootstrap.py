"""
Bootstrap job: download MovieLens 1M, train ALS, seed Redis and PostgreSQL.

Redis keys written:
  item_factor:{movie_id}   → float32 numpy array as bytes (ALS item factors, rank=50)
  item_sim:{movie_id}      → JSON list of {movie_id, similarity} top-20 neighbours
  als_candidates:{user_id} → JSON list of {movie_id, title, genres, als_score, score} top-100
  popular:global           → JSON list of top-100 movies by Bayesian popularity score
  eval:als_metrics         → JSON dict with NDCG@10, Precision@10, n_users_evaluated

PostgreSQL tables written:
  movies          (movie_id, title, genres)
  als_candidates  (user_id, candidates JSONB)
"""
import hashlib
import json
import math
import os
import time
import urllib.request
import zipfile
from typing import Optional

import numpy as np
import psycopg2
import redis as redis_lib
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType

DATA_DIR     = os.getenv("DATA_DIR", "/data")
REDIS_URL    = os.getenv("REDIS_URL", "redis://redis:6379")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://recsys:recsys@postgres:5432/recsys")
MLFLOW_URI   = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def get_spark(master: str = "local[*]") -> SparkSession:
    return (SparkSession.builder
            .master(master)
            .appName("MovieRecSys-Bootstrap")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def download_movielens(data_dir: str = DATA_DIR) -> None:
    ml1m_dir = os.path.join(data_dir, "ml-1m")
    if os.path.exists(os.path.join(ml1m_dir, "ratings.dat")):
        print("MovieLens 1M already present")
        return
    os.makedirs(data_dir, exist_ok=True)
    url = "https://files.grouplens.org/datasets/movielens/ml-1m.zip"
    zip_path = os.path.join(data_dir, "ml-1m.zip")
    print("Downloading MovieLens 1M …")
    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(data_dir)
    os.remove(zip_path)
    print("Done downloading MovieLens 1M")


def load_ratings(spark: SparkSession, data_dir: str = DATA_DIR):
    from pyspark.sql import functions as F
    schema = StructType([
        StructField("userId",    IntegerType()),
        StructField("movieId",   IntegerType()),
        StructField("rating",    FloatType()),
        StructField("timestamp", IntegerType()),
    ])
    path = os.path.join(data_dir, "ml-1m", "ratings.dat")
    df = (spark.read
          .option("sep", "::")
          .schema(schema)
          .csv(path))

    # Data quality assertions — fail loudly rather than train on corrupt data
    null_count = df.filter(
        F.col("userId").isNull() | F.col("movieId").isNull() | F.col("rating").isNull()
    ).count()
    assert null_count == 0, f"Data quality: {null_count} rows with NULL userId/movieId/rating"

    out_of_range = df.filter((F.col("rating") < 1.0) | (F.col("rating") > 5.0)).count()
    assert out_of_range == 0, f"Data quality: {out_of_range} ratings outside [1.0, 5.0]"

    total = df.count()
    assert total >= 100_000, f"Data quality: only {total} ratings — expected at least 100K (MovieLens 1M)"
    print(f"Data quality OK: {total:,} ratings, no nulls, all ratings in [1.0, 5.0]")
    return df


def _extract_year(title: str):
    import re
    m = re.search(r"\((\d{4})\)\s*$", title or "")
    return int(m.group(1)) if m else None


def load_movies(data_dir: str = DATA_DIR) -> dict[int, dict]:
    path = os.path.join(data_dir, "ml-1m", "movies.dat")
    movies: dict[int, dict] = {}
    with open(path, encoding="latin-1") as f:
        for line in f:
            parts = line.strip().split("::")
            movie_id, title, genres = int(parts[0]), parts[1], parts[2]
            movies[movie_id] = {"movie_id": movie_id, "title": title,
                                "genres": genres, "year": _extract_year(title)}
    return movies


def compute_all_popularity(data_dir: str = DATA_DIR) -> dict[int, float]:
    """Bayesian-weighted popularity (avg_rating × log(count + 1)) for EVERY movie.

    Written to the movies.popularity column so the API can order catalogue browse
    results by popularity. Mirrors compute_global_popularity's formula.
    """
    from collections import defaultdict
    path = os.path.join(data_dir, "ml-1m", "ratings.dat")
    counts: dict[int, int]   = defaultdict(int)
    sums:   dict[int, float] = defaultdict(float)
    with open(path, encoding="latin-1") as f:
        for line in f:
            parts    = line.strip().split("::")
            movie_id = int(parts[1])
            rating   = float(parts[2])
            counts[movie_id] += 1
            sums[movie_id]   += rating
    return {
        mid: round((sums[mid] / cnt) * math.log(cnt + 1), 4)
        for mid, cnt in counts.items()
    }


def compute_global_popularity(data_dir: str = DATA_DIR,
                              movies: dict[int, dict] | None = None,
                              top_n: int = 100) -> list[dict]:
    """
    Bayesian-weighted popularity: score = avg_rating × log(count + 1).
    Balances rating quality against volume — avoids pure-count popularity bias.
    """
    from collections import defaultdict
    path = os.path.join(data_dir, "ml-1m", "ratings.dat")
    counts: dict[int, int]   = defaultdict(int)
    sums:   dict[int, float] = defaultdict(float)
    with open(path, encoding="latin-1") as f:
        for line in f:
            parts    = line.strip().split("::")
            movie_id = int(parts[1])
            rating   = float(parts[2])
            counts[movie_id] += 1
            sums[movie_id]   += rating
    popular = []
    for mid, cnt in counts.items():
        avg   = sums[mid] / cnt
        score = round(avg * math.log(cnt + 1), 4)
        meta  = (movies or {}).get(mid, {"title": "Unknown", "genres": ""})
        popular.append({
            "movie_id": mid,
            "title":    meta["title"],
            "genres":   meta["genres"],
            "score":    score,
        })
    popular.sort(key=lambda x: x["score"], reverse=True)
    return popular[:top_n]


def evaluate_model(ratings_df, top_k: int = 10, seed: int = 42) -> dict:
    """
    Leave-one-out offline evaluation: hold out the most recent rating per user,
    retrain ALS on the remainder, compute NDCG@K and Precision@K.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("userId").orderBy(F.desc("timestamp"))
    ranked = ratings_df.withColumn("_rn", F.row_number().over(w))
    train_df = ranked.filter(F.col("_rn") > 1).drop("_rn")
    test_df  = ranked.filter(F.col("_rn") == 1).drop("_rn")

    eligible = (train_df.groupBy("userId").count()
                .filter(F.col("count") >= 5)
                .select("userId"))

    model_eval = train_als(train_df, rank=50, max_iter=10, reg_param=0.1, seed=seed)
    recs_df    = model_eval.recommendForUserSubset(eligible, top_k)

    test_gt: dict[int, set] = {}
    for row in test_df.join(eligible, "userId").collect():
        test_gt.setdefault(row.userId, set()).add(row.movieId)

    ndcg_scores, prec_scores = [], []
    for row in recs_df.collect():
        gt = test_gt.get(row.userId, set())
        if not gt:
            continue
        rec_ids = [r.movieId for r in row.recommendations]
        dcg  = sum(1.0 / math.log2(i + 2) for i, mid in enumerate(rec_ids) if mid in gt)
        idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(gt), top_k)))
        ndcg_scores.append(dcg / idcg if idcg > 0 else 0.0)
        prec_scores.append(sum(1 for mid in rec_ids if mid in gt) / top_k)

    n = len(ndcg_scores)
    return {
        "ndcg@10":           round(sum(ndcg_scores) / n, 4) if n else 0.0,
        "precision@10":      round(sum(prec_scores)  / n, 4) if n else 0.0,
        "n_users_evaluated": n,
        "eval_method":       "leave-one-out (most recent rating held out per user)",
    }


def train_als(ratings_df, rank: int = 50, max_iter: int = 10,
              reg_param: float = 0.1, seed: int = 42):
    als = ALS(
        rank=rank,
        maxIter=max_iter,
        regParam=reg_param,
        seed=seed,
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        coldStartStrategy="drop",
    )
    return als.fit(ratings_df)


def extract_item_factors(model) -> dict[int, np.ndarray]:
    """Return {movie_id: float32 ndarray} for all items in the model."""
    factors: dict[int, np.ndarray] = {}
    for row in model.itemFactors.collect():
        factors[row.id] = np.array(row.features, dtype=np.float32)
    return factors


def compute_item_similarity(model, top_k: int = 20) -> dict[int, list[dict]]:
    """Cosine similarity between all item factor pairs; return top-k per item."""
    item_factors = extract_item_factors(model)
    movie_ids = list(item_factors.keys())
    if not movie_ids:
        return {}
    mat = np.stack([item_factors[mid] for mid in movie_ids])  # (N, rank)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat_norm = mat / norms
    sim_matrix = np.clip(mat_norm @ mat_norm.T, -1.0, 1.0)  # (N, N)
    result: dict[int, list[dict]] = {}
    for i, mid in enumerate(movie_ids):
        row = sim_matrix[i]
        indices = np.argsort(-row)
        neighbors = []
        for j in indices:
            if j == i:
                continue
            neighbors.append({"movie_id": int(movie_ids[j]), "similarity": float(row[j])})
            if len(neighbors) == top_k:
                break
        result[mid] = neighbors
    return result


def get_user_candidates(model, ratings_df, user_ids: Optional[list[int]] = None,
                        n: int = 100) -> dict[int, list[dict]]:
    """Generate top-N ALS candidates per user. Returns {user_id: [{movie_id, als_score}]}."""
    if user_ids:
        spark = ratings_df.sparkSession
        user_df = spark.createDataFrame([(uid,) for uid in user_ids], ["userId"])
        recs_df = model.recommendForUserSubset(user_df, n)
    else:
        recs_df = model.recommendForAllUsers(n)
    result: dict[int, list[dict]] = {}
    for row in recs_df.collect():
        result[int(row.userId)] = [
            {"movie_id": int(r.movieId), "als_score": float(r.rating)}
            for r in row.recommendations
        ]
    return result


def seed_redis(r: redis_lib.Redis, item_factors: dict[int, np.ndarray],
               item_sim: dict[int, list[dict]],
               user_candidates: dict[int, list[dict]],
               movies: dict[int, dict]) -> None:
    pipe = r.pipeline(transaction=False)
    for movie_id, factor in item_factors.items():
        pipe.set(f"item_factor:{movie_id}", factor.tobytes())
    for movie_id, neighbors in item_sim.items():
        pipe.set(f"item_sim:{movie_id}", json.dumps(neighbors))
    for user_id, candidates in user_candidates.items():
        enriched = []
        for c in candidates:
            meta = movies.get(c["movie_id"], {"title": "Unknown", "genres": ""})
            enriched.append({
                "movie_id":  c["movie_id"],
                "title":     meta["title"],
                "genres":    meta["genres"],
                "als_score": c["als_score"],
                "score":     c["als_score"],  # streaming job overwrites this with re-ranked score
            })
        pipe.set(f"als_candidates:{user_id}", json.dumps(enriched))
    popular = compute_global_popularity(movies=movies)
    pipe.set("popular:global", json.dumps(popular))
    pipe.execute()
    print(f"Seeded Redis: {len(item_factors)} item factors, "
          f"{len(item_sim)} item sims, {len(user_candidates)} user candidates, "
          f"{len(popular)} popular movies")


def ensure_schema(conn) -> None:
    """Create all required tables/indexes if missing.

    Postgres only runs schema.sql on first volume init, so a pre-existing volume
    can be missing newer tables (e.g. model_runs) or columns. Running this
    idempotent DDL at bootstrap time makes seeding resilient to any volume state.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS movies (
        movie_id   INTEGER PRIMARY KEY,
        title      TEXT NOT NULL,
        genres     TEXT NOT NULL,
        year       INTEGER,
        popularity DOUBLE PRECISION DEFAULT 0
    );
    ALTER TABLE movies ADD COLUMN IF NOT EXISTS year       INTEGER;
    ALTER TABLE movies ADD COLUMN IF NOT EXISTS popularity DOUBLE PRECISION DEFAULT 0;
    CREATE INDEX IF NOT EXISTS idx_movies_popularity ON movies(popularity DESC);

    CREATE TABLE IF NOT EXISTS als_candidates (
        user_id    INTEGER PRIMARY KEY,
        candidates JSONB   NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS user_recs (
        user_id    INTEGER PRIMARY KEY,
        recs       JSONB   NOT NULL,
        updated_at TIMESTAMP DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_user_recs_updated ON user_recs(updated_at);

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
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print("Schema ensured (created any missing tables/columns)")


def seed_model_run(conn, eval_metrics: dict, rank: int = 50,
                   max_iter: int = 10, reg_param: float = 0.1,
                   seed: int = 42) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE model_runs SET is_active = FALSE")
        cur.execute(
            "INSERT INTO model_runs "
            "(rank, max_iter, reg_param, seed, ndcg_10, precision_10, n_users_eval, is_active) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)",
            (rank, max_iter, reg_param, seed,
             eval_metrics["ndcg@10"], eval_metrics["precision@10"],
             eval_metrics["n_users_evaluated"]),
        )
    conn.commit()
    print("Model run recorded in PostgreSQL model_runs table")


def seed_postgres(conn, movies: dict[int, dict],
                  user_candidates: dict[int, list[dict]],
                  popularity: dict[int, float] | None = None) -> None:
    popularity = popularity or {}
    with conn.cursor() as cur:
        # Self-migrate: ensure new columns exist even on a pre-existing DB volume
        # (schema.sql only runs on first Postgres init, not on an existing volume).
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS year INTEGER")
        cur.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS popularity DOUBLE PRECISION DEFAULT 0")
        cur.executemany(
            "INSERT INTO movies (movie_id, title, genres, year, popularity) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (movie_id) DO UPDATE SET "
            "title = EXCLUDED.title, genres = EXCLUDED.genres, "
            "year = EXCLUDED.year, popularity = EXCLUDED.popularity",
            [(m["movie_id"], m["title"], m["genres"], m.get("year"),
              popularity.get(m["movie_id"], 0.0)) for m in movies.values()],
        )
        cur.executemany(
            "INSERT INTO als_candidates (user_id, candidates) VALUES (%s, %s) "
            "ON CONFLICT (user_id) DO UPDATE SET candidates = EXCLUDED.candidates",
            [(uid, json.dumps(cands)) for uid, cands in user_candidates.items()],
        )
    conn.commit()
    print(f"Seeded PostgreSQL: {len(movies)} movies, {len(user_candidates)} user candidates")


def write_data_metadata(data_dir: str, ratings_path: str, n_ratings: int,
                         n_users: int, n_movies: int) -> dict:
    """Write data lineage metadata to data/metadata.json."""
    sha256 = hashlib.sha256()
    with open(ratings_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    meta = {
        "dataset":       "MovieLens 1M",
        "source_url":    "https://files.grouplens.org/datasets/movielens/ml-1m.zip",
        "ratings_sha256": sha256.hexdigest(),
        "n_ratings":     n_ratings,
        "n_users":       n_users,
        "n_movies":      n_movies,
        "bootstrapped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    out_path = os.path.join(data_dir, "metadata.json")
    with open(out_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Data lineage written to {out_path}")
    return meta


def _mlflow_reachable(uri: str, timeout: float = 3.0) -> bool:
    """TCP probe — returns True only if MLflow server accepts a connection."""
    import socket
    import urllib.parse
    p = urllib.parse.urlparse(uri)
    host = p.hostname or "localhost"
    port = p.port or 5000
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def main() -> None:
    # MLflow experiment tracking — probe first, import only when reachable
    run = None
    if _mlflow_reachable(MLFLOW_URI):
        try:
            import mlflow
            mlflow.set_tracking_uri(MLFLOW_URI)
            mlflow.set_experiment("movie-recsys-als")
            run = mlflow.start_run()
            print(f"MLflow run started: {run.info.run_id} (tracking URI: {MLFLOW_URI})")
        except Exception as e:
            print(f"MLflow init failed ({e}), continuing without experiment tracking")
    else:
        print(f"MLflow not reachable at {MLFLOW_URI}, continuing without experiment tracking")

    rank, max_iter, reg_param, seed = 50, 10, 0.1, 42

    download_movielens()
    spark = get_spark()
    movies = load_movies()
    ratings_df = load_ratings(spark)

    # Write data lineage
    n_ratings = ratings_df.count()
    n_users   = ratings_df.select("userId").distinct().count()
    n_movies  = ratings_df.select("movieId").distinct().count()
    ratings_path = os.path.join(DATA_DIR, "ml-1m", "ratings.dat")
    data_meta = write_data_metadata(DATA_DIR, ratings_path, n_ratings, n_users, n_movies)

    print("Running offline evaluation (leave-one-out) …")
    eval_metrics = evaluate_model(ratings_df)
    print(f"ALS offline metrics: {eval_metrics}")

    # Train on full dataset — distinct from evaluate_model which uses leave-one-out split.
    print(f"Training ALS (rank={rank}, maxIter={max_iter}) …")
    model = train_als(ratings_df, rank=rank, max_iter=max_iter,
                      reg_param=reg_param, seed=seed)

    print("Extracting item factors and computing item similarity …")
    item_factors = extract_item_factors(model)
    item_sim = compute_item_similarity(model, top_k=20)
    print("Generating user candidates …")
    user_candidates = get_user_candidates(model, ratings_df, n=100)

    print("Computing global popularity ranking …")
    r = redis_lib.Redis.from_url(REDIS_URL)
    seed_redis(r, item_factors, item_sim, user_candidates, movies)
    r.set("eval:als_metrics", json.dumps(eval_metrics))

    # Write model version key — streaming job polls this to detect when to hot-reload
    model_version = str(int(time.time()))
    r.set("model:version", model_version)
    print(f"Model version key set: model:version={model_version}")

    print("Eval metrics stored in Redis → GET /stats/ml")
    conn = psycopg2.connect(POSTGRES_URL)
    ensure_schema(conn)
    print("Computing full-catalogue popularity for browse ordering …")
    all_popularity = compute_all_popularity()
    seed_postgres(conn, movies, user_candidates, all_popularity)
    seed_model_run(conn, eval_metrics, rank=rank, max_iter=max_iter,
                   reg_param=reg_param, seed=seed)
    conn.close()

    # Log everything to MLflow
    if run is not None:
        try:
            mlflow.log_params({
                "rank": rank, "maxIter": max_iter, "regParam": reg_param, "seed": seed,
                "n_ratings": n_ratings, "n_users": n_users, "n_movies": n_movies,
                "dataset": data_meta["dataset"],
            })
            mlflow.log_metrics({
                "ndcg_at_10":      eval_metrics["ndcg@10"],
                "precision_at_10": eval_metrics["precision@10"],
                "n_users_evaluated": eval_metrics["n_users_evaluated"],
            })
            mlflow.log_artifact(os.path.join(DATA_DIR, "metadata.json"))
            mlflow.set_tag("model_version", model_version)
            mlflow.end_run()
            print("MLflow run logged successfully")
        except Exception as e:
            print(f"MLflow logging failed ({e}), continuing")

    spark.stop()
    print("Bootstrap complete.")


if __name__ == "__main__":
    main()
