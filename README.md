---
title: CineMatch Lite
emoji: 🎬
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 7860
pinned: false
---

# CineMatch Lite — HuggingFace Spaces demo

> **🚀 Live Demo:** [huggingface.co/spaces/Amit-kr26/Cinematch](https://huggingface.co/spaces/Amit-kr26/Cinematch) — this branch is what runs there.

Single-container, SQLite-backed version of [CineMatch](https://github.com/Amit-kr26/Cinematch)
(the full Kafka → Spark → ALS streaming stack lives on the `main` branch).
Cold start is instant: recommendations ship pre-trained in `app/seed/als_seed.sqlite.gz`
(6040 users × top-10 ALS picks + catalogue + cached TMDB art).

## What's kept vs the full stack

| | Full (`main`) | Lite (this branch) |
|---|---|---|
| Serving | FastAPI · 4-layer fallback | FastAPI · 3-layer fallback |
| Personalization | Spark streaming re-rank every 10 s | in-process numpy re-rank on every event (same scoring math) |
| Infra | Kafka · Redis · Postgres · MLflow | one SQLite file |
| Posters | live TMDB enrichment | seed art + live TMDB when `TMDB_API_KEY` secret is set |

Events are not just logged: each `/simulate` recomputes the user's top-10
(time-decayed preference vector over ALS item factors + genre bonus +
diversity penalty) and pushes it to any open WebSocket instantly.

## Endpoints

`GET /recommend/{user_id}` · `POST /simulate` · `GET /movies` · `GET /genres` · `GET /stats` · `GET /health` · `GET /metrics`

## Config

| Env | Default | Purpose |
|-----|---------|---------|
| `PORT` | `7860` | HTTP port (HF Spaces sets this) |
| `DB_PATH` | `/data/app.db` | SQLite database location |
| `DATA_DIR` | `/data` | working dir if retraining |
| `TMDB_API_KEY` | unset | optional: fetch missing posters/backdrops/overviews live |

## Regenerating the seed

Run the full stack once (`make bootstrap` on `main`, then snapshot Redis), then:

```bash
python scripts/build_seed.py --snapshot data/redis_snapshot.json.gz \
    --movies data/ml-1m/movies.dat --ratings data/ml-1m/ratings.dat
```

Retraining from scratch inside this container is possible via
`python -m app.bootstrap_lite` but requires adding PySpark + a JRE to the image.
