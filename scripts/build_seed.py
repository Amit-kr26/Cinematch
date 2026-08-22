"""Build app/seed/als_seed.sqlite.gz from full-stack artifacts.

Inputs (produced by the full deployment on the main branch):
  --snapshot  Redis snapshot JSON (infra/hf/dump_redis.py output)
  --movies    MovieLens ml-1m/movies.dat
  --ratings   MovieLens ml-1m/ratings.dat

Usage:
  python scripts/build_seed.py \
      --snapshot data/redis_snapshot.json.gz \
      --movies data/ml-1m/movies.dat --ratings data/ml-1m/ratings.dat
"""
import argparse
import gzip
import json
import math
import re
import sqlite3
import tempfile
from collections import defaultdict
from pathlib import Path

SEED_OUT = Path(__file__).resolve().parent.parent / "app" / "seed" / "als_seed.sqlite.gz"
YEAR_RE = re.compile(r"\((\d{4})\)\s*$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--movies", required=True)
    parser.add_argument("--ratings", required=True)
    args = parser.parse_args()

    opener = gzip.open if str(args.snapshot).endswith(".gz") else open
    with opener(args.snapshot, "rt", encoding="utf-8") as f:
        snap = json.load(f)

    movies: dict[int, dict] = {}
    with open(args.movies, encoding="latin-1") as f:
        for line in f:
            mid, title, genres = line.strip().split("::")
            m = YEAR_RE.search(title)
            movies[int(mid)] = {"title": title, "genres": genres,
                                "year": int(m.group(1)) if m else None}

    counts: dict[int, int] = defaultdict(int)
    sums: dict[int, float] = defaultdict(float)
    with open(args.ratings, encoding="latin-1") as f:
        for line in f:
            _, mid, rating, _ = line.strip().split("::")
            counts[mid] += 1
            sums[mid] += float(rating)

    def pop(mid: int) -> float:
        return round((sums[mid] / counts[mid]) * math.log(counts[mid] + 1), 4) \
               if counts.get(mid) else 0.0

    def snap_json(key: str):
        raw = snap.get(key)
        return json.loads(raw["value"]) if raw else None

    tmdb: dict[int, dict] = {}
    for key, entry in snap.items():
        if key.startswith("tmdb:") and entry["type"] == "string":
            try:
                tmdb[int(key.split(":")[1])] = json.loads(entry["value"])
            except (ValueError, KeyError):
                pass

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = Path(tmp.name)

    conn = sqlite3.connect(db_path)
    from app.bootstrap_lite import SCHEMA
    conn.executescript(SCHEMA)

    conn.executemany(
        "INSERT OR REPLACE INTO movies VALUES (?,?,?,?,?,?,?,?,?)",
        [(mid, m["title"], m["genres"], m["year"], pop(mid),
          t.get("poster"), t.get("backdrop"), t.get("overview") or "",
          t.get("tmdb_rating", 0.0))
         for mid, m in sorted(movies.items())
         for t in [tmdb.get(mid, {})]])

    n_users = 0
    for key, entry in snap.items():
        if not key.startswith("als_candidates:"):
            continue
        uid = int(key.split(":")[1])
        cands = json.loads(entry["value"])[:10]
        conn.execute("INSERT OR REPLACE INTO als_candidates VALUES (?,?)",
                     (uid, json.dumps(cands)))
        n_users += 1

    popular = snap_json("popular:global") or []
    conn.executemany(
        "INSERT OR REPLACE INTO popular VALUES (?,?,?,?,?)",
        [(i, p["movie_id"],
          movies.get(p["movie_id"], {}).get("title", p.get("title", "")),
          movies.get(p["movie_id"], {}).get("genres", p.get("genres", "")),
          p.get("score", 0.0))
         for i, p in enumerate(popular[:100])])

    conn.commit()
    conn.close()

    SEED_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(db_path, "rb") as src, gzip.open(SEED_OUT, "wb", compresslevel=9) as dst:
        dst.write(src.read())
    db_path.unlink()
    print(f"Seed written: {SEED_OUT} "
          f"({SEED_OUT.stat().st_size / 1_048_576:.1f} MB, {n_users} users)")


if __name__ == "__main__":
    main()
