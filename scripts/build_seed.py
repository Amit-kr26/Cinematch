"""Build app/seed/als_seed.sqlite.gz from full-stack artifacts.

Inputs (produced by the full deployment on the main branch):
  --snapshot  Redis snapshot JSON (infra/hf/dump_redis.py output)
  --movies    MovieLens ml-1m/movies.dat
  --ratings   MovieLens ml-1m/ratings.dat
Optional:
  --tmdb-key  fetch posters/backdrops/overviews for every movie missing
              cached art and bake them into the seed (HF Spaces FS is
              ephemeral — anything not in the seed is lost on restart)

Usage:
  python scripts/build_seed.py \
      --snapshot data/redis_snapshot.json.gz \
      --movies data/ml-1m/movies.dat --ratings data/ml-1m/ratings.dat \
      [--tmdb-key $TMDB_API_KEY]
"""
import argparse
import gzip
import json
import math
import os
import re
import sqlite3
import tempfile
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SEED_OUT = Path(__file__).resolve().parent.parent / "app" / "seed" / "als_seed.sqlite.gz"
YEAR_RE    = re.compile(r"\((\d{4})\)\s*$")
ARTICLE_RE = re.compile(
    r"^(.*),\s+(The|A|An|Les|La|Le|Los|Las|Un|Une|Der|Die|Das|Il|Lo)$", re.I)


def clean_title(raw: str) -> str:
    stripped = YEAR_RE.sub("", raw or "").strip()
    m = ARTICLE_RE.match(stripped)
    return f"{m.group(2)} {m.group(1)}" if m else stripped


def tmdb_search(key: str, title: str, year: int | None) -> dict | None:
    params = {"api_key": key, "query": clean_title(title)}
    if year:
        params["year"] = year
    url = ("https://api.themoviedb.org/3/search/movie?"
           + urllib.parse.urlencode(params))
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                results = json.load(r).get("results", [])
            if not results and year:
                params.pop("year", None)
                url = ("https://api.themoviedb.org/3/search/movie?"
                       + urllib.parse.urlencode(params))
                continue
            best = results[0] if results else None
            if not best:
                return {}
            out = {"poster":   "https://image.tmdb.org/t/p/w342" + best["poster_path"]
                              if best.get("poster_path") else None,
                   "backdrop": "https://image.tmdb.org/t/p/w780" + best["backdrop_path"]
                              if best.get("backdrop_path") else None,
                   "overview": best.get("overview") or "",
                   "tmdb_rating": best.get("vote_average") or 0.0}
            if not out["poster"] and year:      # retry once without the year
                time.sleep(0.2)
                params = {"api_key": key, "query": clean_title(title)}
                with urllib.request.urlopen(
                        "https://api.themoviedb.org/3/search/movie?"
                        + urllib.parse.urlencode(params), timeout=10) as r:
                    alt = (json.load(r).get("results") or [{}])[0]
                if alt.get("poster_path"):
                    out["poster"] = ("https://image.tmdb.org/t/p/w342"
                                     + alt["poster_path"])
            return out
        except Exception:
            if attempt == 2:
                return None
            time.sleep(0.5 * (attempt + 1))
    return None


def enrich_missing(key: str, movies: dict[int, dict],
                   tmdb_map: dict[int, dict]) -> int:
    todo = [mid for mid, m in movies.items() if not tmdb_map.get(mid, {}).get("poster")]
    print(f"TMDB sweep: fetching art for {len(todo)} movies …")
    done = 0

    def work(mid: int):
        m = movies[mid]
        return mid, tmdb_search(key, m["title"], m.get("year"))

    with ThreadPoolExecutor(max_workers=6) as pool:
        for i, (mid, data) in enumerate(pool.map(work, todo), 1):
            if data:
                tmdb_map[mid] = {**tmdb_map.get(mid, {}), **data}
                done += 1
            if i % 250 == 0:
                print(f"  {i}/{len(todo)} ({done} matched)")
    print(f"TMDB sweep complete: {done}/{len(todo)} matched")
    return done


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--movies", required=True)
    parser.add_argument("--ratings", required=True)
    parser.add_argument("--tmdb-key", default=os.getenv("TMDB_API_KEY", ""),
                        help="fetch art for movies missing cached TMDB entries")
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
            counts[int(mid)] += 1
            sums[int(mid)] += float(rating)

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

    if args.tmdb_key:
        enrich_missing(args.tmdb_key, movies, tmdb)

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

    import base64
    n_factors = 0
    for key, entry in snap.items():
        if not key.startswith("item_factor:") or entry["type"] != "bytes":
            continue
        conn.execute("INSERT OR REPLACE INTO item_factor VALUES (?,?)",
                     (int(key.split(":")[1]), base64.b64decode(entry["value"])))
        n_factors += 1

    n_sims = 0
    for key, entry in snap.items():
        if not key.startswith("item_sim:") or entry["type"] != "string":
            continue
        conn.execute("INSERT OR REPLACE INTO item_sim VALUES (?,?)",
                     (int(key.split(":")[1]), entry["value"]))
        n_sims += 1

    conn.commit()
    conn.close()

    SEED_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(db_path, "rb") as src, gzip.open(SEED_OUT, "wb", compresslevel=9) as dst:
        dst.write(src.read())
    db_path.unlink()
    print(f"Seed written: {SEED_OUT} "
          f"({SEED_OUT.stat().st_size / 1_048_576:.1f} MB, {n_users} users, "
          f"{n_factors} item factors, {n_sims} sim maps)")


if __name__ == "__main__":
    main()
