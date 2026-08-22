"""CineMatch Lite — SQLite bootstrap.

Startup priority:
1. Database already present  -> nothing to do.
2. Bundled seed snapshot     -> restore it (instant cold start).
3. Neither                   -> download MovieLens 1M and train ALS locally
                                (needs PySpark + a JVM; several minutes).
"""
import gzip
import json
import math
import os
import shutil
import sqlite3
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB_PATH  = Path(os.getenv("DB_PATH", "/data/app.db"))
SEED_GZ  = Path(__file__).resolve().parent / "seed" / "als_seed.sqlite.gz"

SCHEMA = """
CREATE TABLE IF NOT EXISTS movies (
    movie_id   INTEGER PRIMARY KEY,
    title      TEXT NOT NULL,
    genres     TEXT NOT NULL,
    year       INTEGER,
    popularity REAL DEFAULT 0,
    poster     TEXT,
    backdrop   TEXT,
    overview   TEXT DEFAULT '',
    tmdb_rating REAL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS als_candidates (
    user_id    INTEGER PRIMARY KEY,
    candidates TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS user_recs (
    user_id    INTEGER PRIMARY KEY,
    recs       TEXT NOT NULL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS popular (
    rank     INTEGER PRIMARY KEY,
    movie_id INTEGER NOT NULL,
    title    TEXT NOT NULL,
    genres   TEXT NOT NULL,
    score    REAL NOT NULL
);
"""


def ensure_db() -> None:
    """Idempotent: make sure DB_PATH holds a seeded database before serving."""
    if DB_PATH.exists():
        print(f"Database already seeded: {DB_PATH}")
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if SEED_GZ.exists():
        print(f"Restoring bundled seed -> {DB_PATH} …")
        with gzip.open(SEED_GZ, "rb") as src, open(DB_PATH, "wb") as dst:
            shutil.copyfileobj(src, dst)
        print("Seed restored.")
        return
    print("No seed bundle found — training ALS on MovieLens 1M …")
    _train_and_seed()
    print("Training complete, database seeded.")


# ── Train-from-scratch path (optional; bundled seed makes this unnecessary) ──

def _download_movielens() -> Path:
    ml_dir = DATA_DIR / "ml-1m"
    if (ml_dir / "ratings.dat").exists():
        return ml_dir
    ml_dir.mkdir(parents=True, exist_ok=True)
    zip_path = DATA_DIR / "ml-1m.zip"
    print("Downloading MovieLens 1M …")
    urllib.request.urlretrieve(
        "https://files.grouplens.org/datasets/movielens/ml-1m.zip", zip_path)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(DATA_DIR)
    os.remove(zip_path)
    return ml_dir


def _load_movies(ml_dir: Path) -> dict[int, dict]:
    import re
    year_re = re.compile(r"\((\d{4})\)\s*$")
    movies: dict[int, dict] = {}
    with open(ml_dir / "movies.dat", encoding="latin-1") as f:
        for line in f:
            mid, title, genres = line.strip().split("::")
            m = year_re.search(title)
            movies[int(mid)] = {
                "title": title, "genres": genres,
                "year": int(m.group(1)) if m else None,
            }
    return movies


def _train_and_seed() -> None:
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import SparkSession, types

    ml_dir = _download_movielens()

    spark = (SparkSession.builder.master("local[*]")
             .appName("CineMatch-Lite")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    schema = types.StructType([
        types.StructField("userId", types.IntegerType()),
        types.StructField("movieId", types.IntegerType()),
        types.StructField("rating", types.FloatType()),
        types.StructField("timestamp", types.IntegerType()),
    ])
    ratings = spark.read.option("sep", "::").schema(schema).csv(str(ml_dir / "ratings.dat"))
    model = ALS(rank=50, maxIter=10, regParam=0.1, seed=42,
                userCol="userId", itemCol="movieId", ratingCol="rating",
                coldStartStrategy="drop").fit(ratings)
    recs = model.recommendForAllUsers(10).collect()
    spark.stop()

    movies = _load_movies(ml_dir)
    counts: dict[int, int] = defaultdict(int)
    sums: dict[int, float] = defaultdict(float)
    with open(ml_dir / "ratings.dat", encoding="latin-1") as f:
        for line in f:
            _, mid, rating, _ = line.strip().split("::")
            counts[int(mid)] += 1
            sums[int(mid)] += float(rating)

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT OR REPLACE INTO movies VALUES (?,?,?,?,?,?,?,'',0)",
        [(mid, m["title"], m["genres"], m["year"],
          round((sums[mid] / cnt) * math.log(cnt + 1), 4), None, None)
         for mid, cnt in counts.items() if (m := movies.get(mid))])
    conn.executemany(
        "INSERT OR REPLACE INTO als_candidates VALUES (?,?)",
        [(int(row.userId), json.dumps([
            {"movie_id": int(r.movieId),
             "title":    movies.get(int(r.movieId), {}).get("title", ""),
             "genres":   movies.get(int(r.movieId), {}).get("genres", ""),
             "score":    float(r.rating)}
            for r in row.recommendations]))
         for row in recs])
    popular = sorted(
        ({"movie_id": mid, **movies.get(mid, {"title": "?", "genres": ""}),
          "score": round((sums[mid] / cnt) * math.log(cnt + 1), 4)}
         for mid, cnt in counts.items() if mid in movies),
        key=lambda x: x["score"], reverse=True)[:100]
    conn.executemany(
        "INSERT OR REPLACE INTO popular VALUES (?,?,?,?,?)",
        [(i, p["movie_id"], p["title"], p["genres"], p["score"])
         for i, p in enumerate(popular)])
    conn.commit()
    conn.close()


if __name__ == "__main__":
    ensure_db()
