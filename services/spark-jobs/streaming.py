"""
Spark Structured Streaming job.

Reads Kafka topic `user-events` in 10-second micro-batches.
Watermark: 30 seconds declared (required for event-time semantics in Structured Streaming). With foreachBatch, late-event filtering is applied manually in process_batch; state is bounded by MAX_EVENTS=50 in Redis.
State: per-user event history maintained in Redis `user_state:{user_id}` (JSON, last 50 events).
Re-ranking: time-decay weighted preference vector (exp(-λ * age_seconds)) dot-producted against
            ALS item factors to re-score ALS top-100 candidates → top-10 written to Redis + PostgreSQL.

Redis keys read/written:
  user_state:{user_id}     → JSON list of {movie_id, ts, event_type} (last 50 events)
  item_factor:{movie_id}   → raw float32 bytes (written by bootstrap)
  als_candidates:{user_id} → JSON list of {movie_id, title, genres, als_score, score}
  recs:{user_id}           → JSON list of top-10 {movie_id, title, genres, score}, TTL 5min

PostgreSQL table written:
  user_recs (user_id, recs JSONB, updated_at)
"""
import json
import math
import os
import random
import time
from datetime import datetime, timezone

import numpy as np
import psycopg2
import redis as redis_lib
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "user-events")
REDIS_URL       = os.getenv("REDIS_URL", "redis://redis:6379")
POSTGRES_URL    = os.getenv("POSTGRES_URL", "postgresql://recsys:recsys@postgres:5432/recsys")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")
LAMBDA_DECAY    = 0.001   # λ for exp(-λ * age_seconds) time-decay weighting
MAX_EVENTS      = 50      # max events kept per user in Redis state

# Explicit feedback outweighs implicit signals; ratings also scale by star value
EVENT_TYPE_WEIGHTS: dict[str, float] = {
    "rating": 3.0,
    "like":   2.5,
    "view":   1.5,
    "click":  1.0,
}

GENRE_HYBRID_WEIGHT = 0.15   # α: genre signal blend weight in final score
MAX_PER_GENRE       = 3      # diversity cap: max movies per primary genre in top-10
SIM_EXPANSION_K     = 3      # top-K item_sim neighbors per recently-viewed movie
SIM_EXPANSION_MAX   = 30     # max total candidates added via item_sim expansion

VALID_EVENT_TYPES = frozenset(EVENT_TYPE_WEIGHTS.keys())


# ── Pure functions (testable without Spark or network I/O) ──────────────────

def merge_events(existing: list[dict], new_events: list[dict],
                 max_events: int = MAX_EVENTS) -> list[dict]:
    """Merge existing + new events, FIFO-cap at max_events."""
    combined = existing + new_events
    return combined[-max_events:]


def compute_genre_preferences(events: list[dict],
                              movie_metadata: dict[int, dict]) -> dict[str, float]:
    """
    Build user genre preference weights from interaction history.
    Weights each genre by EVENT_TYPE_WEIGHTS; normalizes to sum=1.
    Primary genre (first in pipe-separated list) receives 2× weight vs secondary genres
    to avoid signal dilution from multi-genre movies.
    """
    genre_weights: dict[str, float] = {}
    for evt in events:
        meta   = movie_metadata.get(evt["movie_id"], {})
        genres = [g for g in (meta.get("genres") or "").split("|") if g]
        weight = EVENT_TYPE_WEIGHTS.get(evt.get("event_type", "click"), 1.0)
        for i, g in enumerate(genres):
            # Primary genre (i=0) gets 2× multiplier; secondary genres get 1×
            multiplier = 2.0 if i == 0 else 1.0
            genre_weights[g] = genre_weights.get(g, 0.0) + weight * multiplier
    total = sum(genre_weights.values())
    if total > 0:
        genre_weights = {g: w / total for g, w in genre_weights.items()}
    return genre_weights


def _validate_event(user_id, movie_id, event_type) -> tuple[bool, str | None]:
    """Return (True, None) for valid events; (False, reason) for invalid."""
    if not user_id or int(user_id) <= 0:
        return False, "invalid_user_id"
    if not movie_id or int(movie_id) <= 0:
        return False, "invalid_movie_id"
    if event_type not in VALID_EVENT_TYPES:
        return False, f"unknown_event_type:{event_type}"
    return True, None


def compute_preference_vector(events: list[dict], item_factors: dict[int, np.ndarray],
                               now_ts: float,
                               lambda_decay: float = LAMBDA_DECAY) -> np.ndarray:
    """
    Weighted sum of item factors using time-decay and event-type multipliers.

    weight = exp(-λ * age) × type_weight × rating_weight
      type_weight:   rating=3.0, view=1.5, click=1.0
      rating_weight: star_value / 5.0  (only for explicit ratings; else 1.0)

    Returns a unit-normalised float32 vector; zero vector if no events.
    """
    if not events or not item_factors:
        rank = next(iter(item_factors.values())).shape[0] if item_factors else 0
        return np.zeros(rank, dtype=np.float32)

    rank = next(iter(item_factors.values())).shape[0]
    pref = np.zeros(rank, dtype=np.float32)
    for evt in events:
        factor = item_factors.get(evt["movie_id"])
        if factor is None:
            continue
        age          = max(0.0, now_ts - evt["ts"])
        time_weight  = math.exp(-lambda_decay * age)
        type_weight  = EVENT_TYPE_WEIGHTS.get(evt.get("event_type", "click"), 1.0)
        # For explicit ratings, ensure any star value outweighs implicit feedback.
        # Formula maps 1-star→0.52×, 5-star→1.00×; all ratings beat view (1.5) when combined
        # with the 3.0 type_weight (minimum effective weight: 3.0×0.52=1.56 > view 1.5).
        rating_weight = 1.0
        if evt.get("event_type") == "rating" and evt.get("rating"):
            rating_weight = 0.4 + float(evt["rating"]) / 5.0 * 0.6
        pref += (time_weight * type_weight * rating_weight) * factor

    norm = np.linalg.norm(pref)
    if norm > 0:
        pref /= norm
    return pref


def rerank_candidates(pref_vector: np.ndarray, candidates: list[dict],
                      item_factors: dict[int, np.ndarray],
                      user_genre_prefs: dict[str, float] | None = None,
                      top_n: int = 10,
                      max_per_genre: int = MAX_PER_GENRE) -> list[dict]:
    """
    Score candidates by dot product against pref_vector (hybrid: + genre bonus).
    Falls back to als_score when item factor missing.
    Applies soft genre diversity via exponential score penalty after max_per_genre threshold
    (penalises over-represented genres without hard-excluding user-preferred content).
    Returns top_n items.
    """
    scored = []
    for cand in candidates:
        factor   = item_factors.get(cand["movie_id"])
        cf_score = float(np.dot(pref_vector, factor)) if factor is not None else cand.get("als_score", 0.0)
        genre_bonus = 0.0
        if user_genre_prefs:
            genres      = (cand.get("genres") or "").split("|")
            genre_bonus = sum(user_genre_prefs.get(g, 0.0) for g in genres if g) * GENRE_HYBRID_WEIGHT
        scored.append({**cand, "score": cf_score + genre_bonus})
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Soft diversity: exponential score penalty for over-represented primary genres.
    # Sign-safe: CF dot products against a unit pref vector are frequently
    # negative, and multiplying a negative score by 0.7 would *raise* it.
    # Scaling |score| preserves ordering intent for both signs.
    # Applies from the 4th title of a genre onward (4th ×0.7, 5th ×0.49 …).
    genre_counts: dict[str, int] = {}
    result = []
    for item in scored:
        primary = (item.get("genres") or "").split("|")[0] or "Unknown"
        count = genre_counts.get(primary, 0)
        if count >= max_per_genre:
            factor = 0.7 ** (count - max_per_genre + 1)
            raw = item["score"]
            penalized = math.copysign(abs(raw) * factor, raw)
            item = {**item, "score": penalized}
        genre_counts[primary] = count + 1
        result.append(item)

    result.sort(key=lambda x: x["score"], reverse=True)
    return result[:top_n]


# ── DB helpers ──────────────────────────────────────────────────────────────

def _get_pg_conn(postgres_url: str, existing_conn):
    """Return existing conn if open, else reconnect."""
    try:
        existing_conn.cursor().execute("SELECT 1")
        return existing_conn
    except Exception:
        try:
            existing_conn.close()
        except Exception:
            pass
        return psycopg2.connect(postgres_url)


# ── I/O helpers ─────────────────────────────────────────────────────────────

def load_item_factors(r: redis_lib.Redis, movie_ids: list[int]) -> dict[int, np.ndarray]:
    """Bulk-fetch item factors from Redis."""
    pipe = r.pipeline()
    for mid in movie_ids:
        pipe.get(f"item_factor:{mid}")
    results = pipe.execute()
    factors: dict[int, np.ndarray] = {}
    for mid, raw in zip(movie_ids, results):
        if raw:
            factors[mid] = np.frombuffer(raw, dtype=np.float32).copy()
    return factors


def load_item_sim(r: redis_lib.Redis, movie_ids: list[int]) -> dict[int, list[dict]]:
    """Bulk-fetch item similarity maps from Redis."""
    pipe = r.pipeline()
    for mid in movie_ids:
        pipe.get(f"item_sim:{mid}")
    sim_map: dict[int, list[dict]] = {}
    for mid, raw in zip(movie_ids, pipe.execute()):
        if raw:
            sim_map[mid] = json.loads(raw)
    return sim_map


def process_batch(batch_df, batch_id: int, r: redis_lib.Redis,
                  pg_conn, all_item_factors: dict[int, np.ndarray],
                  all_item_sim: dict[int, list[dict]],
                  all_movie_meta: dict[int, dict]) -> None:
    """
    foreachBatch handler: update user state + recs for all users in the batch.
    Runs on the Spark driver (local[*] mode).
    """
    rows = batch_df.collect()
    if not rows:
        print(f"Batch {batch_id}: empty, skipping")
        return

    events_by_user: dict[int, list[dict]] = {}
    for row in rows:
        ts = (row.timestamp.timestamp()
              if hasattr(row.timestamp, "timestamp")
              else float(row.timestamp))
        valid, reason = _validate_event(row.user_id, row.movie_id, row.event_type)
        if not valid:
            r.incr("dlq:count")
            r.lpush("dlq:recent", json.dumps({
                "user_id": row.user_id, "movie_id": row.movie_id,
                "event_type": row.event_type, "reason": reason, "ts": ts,
            }))
            r.ltrim("dlq:recent", 0, 99)
            continue
        uid = int(row.user_id)
        events_by_user.setdefault(uid, []).append({
            "movie_id":   int(row.movie_id),
            "ts":         ts,
            "event_type": row.event_type,
        })

    now_ts = time.time()
    written = 0

    # Bulk-prefetch user state and ALS candidates — avoids N×2 Redis round-trips
    user_ids = list(events_by_user.keys())
    pipe = r.pipeline()
    for uid in user_ids:
        pipe.get(f"user_state:{uid}")
    prefetched_state = dict(zip(user_ids, pipe.execute()))
    pipe = r.pipeline()
    for uid in user_ids:
        pipe.get(f"als_candidates:{uid}")
    prefetched_cands = dict(zip(user_ids, pipe.execute()))

    # NOTE on integrity: only the PostgreSQL writes are transactional, and
    # each user commits independently. Redis state/recs/publish are NOT
    # rolled back on failure — accepted because serving reads Redis and
    # every write is idempotent.
    written = 0
    ttl = 300 + random.randint(-30, 30)   # jitter avoids synchronized expiry
    ts_iso = datetime.now(timezone.utc).isoformat()

    cur = pg_conn.cursor()
    try:
        for user_id, new_events in events_by_user.items():
            # Per-user isolation: one bad user (corrupt state, Redis hiccup,
            # bad JSON) must not abort the rest of the micro-batch.
            try:
                new_events = [e for e in new_events if now_ts - e["ts"] <= 300]
                if not new_events:
                    continue

                state_bytes = prefetched_state.get(user_id)
                existing = json.loads(state_bytes) if state_bytes else []

                merged = merge_events(existing, new_events)
                r.set(f"user_state:{user_id}", json.dumps(merged), ex=3600)

                pref = compute_preference_vector(merged, all_item_factors, now_ts)

                cand_bytes = prefetched_cands.get(user_id)
                if not cand_bytes:
                    continue
                candidates = json.loads(cand_bytes)

                # Item-sim candidate expansion. Cosine similarity ∈ [-1,1] is
                # NOT comparable to ALS predicted ratings (~1-5): it is stored
                # under `similarity`, deliberately OUT of `als_score`, so the
                # factor-missing fallback scores these at 0 instead of mixing
                # incomparable scales.
                recent_ids = list(dict.fromkeys(
                    e["movie_id"] for e in reversed(merged)
                ))[:5]
                candidate_ids = {c["movie_id"] for c in candidates if "movie_id" in c}
                extra: list[dict] = []
                for mid in recent_ids:
                    for sim_entry in all_item_sim.get(mid, [])[:SIM_EXPANSION_K]:
                        sim_id = sim_entry["movie_id"]
                        if sim_id not in candidate_ids and sim_id in all_movie_meta:
                            meta = all_movie_meta[sim_id]
                            extra.append({
                                "movie_id":   sim_id,
                                "title":      meta["title"],
                                "genres":     meta["genres"],
                                "similarity": sim_entry["similarity"],
                                "expanded":   True,
                            })
                            candidate_ids.add(sim_id)
                    if len(extra) >= SIM_EXPANSION_MAX:
                        break
                if extra:
                    candidates = candidates + extra

                genre_prefs = compute_genre_preferences(merged, all_movie_meta)
                top10 = rerank_candidates(pref, candidates, all_item_factors,
                                          user_genre_prefs=genre_prefs, top_n=10)

                r.set(f"recs:{user_id}", json.dumps(top10), ex=ttl)
                # Freshness timestamp consumed by /recommend — without it the
                # UI would always show "Updated just now".
                r.set(f"recs:{user_id}:ts", ts_iso, ex=ttl)
                r.publish(f"recs_update:{user_id}", "1")

                cur.execute(
                    "INSERT INTO user_recs (user_id, recs, updated_at) VALUES (%s, %s, NOW()) "
                    "ON CONFLICT (user_id) DO UPDATE "
                    "SET recs = EXCLUDED.recs, updated_at = NOW()",
                    (user_id, json.dumps(top10)),
                )
                pg_conn.commit()
                written += 1
            except Exception as ue:
                print(f"Batch {batch_id}: user {user_id} failed — {ue}")
                try:
                    pg_conn.rollback()
                except Exception:
                    pass
    finally:
        try:
            cur.close()
        except Exception:
            pass
    print(f"Batch {batch_id}: processed {len(events_by_user)} users, "
          f"Wrote {written} recs to Redis")


def ensure_topic(bootstrap_servers: str, topic: str, num_partitions: int = 3) -> None:
    """Create the Kafka topic and wait until its metadata becomes visible.

    Without this wait, Spark's KafkaMicroBatchStream raises
    UnknownTopicOrPartitionException during initial offset fetch. Note: the
    readiness check confirms partitions EXIST — it does not verify that every
    partition has an elected leader (metadata propagation usually wins that
    race; a leaderless partition would still fail the first offset fetch).
    """
    from kafka import KafkaConsumer

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers.split(","))
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
        print(f"Created Kafka topic '{topic}'")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists")
    finally:
        admin.close()

    # Wait until all partitions are assigned (up to 30 s)
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers.split(","))
    for i in range(30):
        try:
            parts = consumer.partitions_for_topic(topic)
            if parts and len(parts) >= 1:
                print(f"Topic '{topic}' ready: {len(parts)} partition(s)")
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        print(f"Warning: topic '{topic}' may not be fully ready")
    consumer.close()


def main() -> None:
    ensure_topic(KAFKA_SERVERS, KAFKA_TOPIC)

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("MovieRecSys-Streaming")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
             .config("spark.sql.shuffle.partitions", "3")
             .config("spark.ui.enabled", "false")
             .getOrCreate())

    event_schema = StructType([
        StructField("user_id",    IntegerType()),
        StructField("movie_id",   IntegerType()),
        StructField("event_type", StringType()),
        StructField("rating",     FloatType()),
        StructField("timestamp",  TimestampType()),
    ])

    raw_stream = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_SERVERS)
                  .option("subscribe", KAFKA_TOPIC)
                  .option("startingOffsets", "latest")
                  .option("failOnDataLoss", "false")
                  .load())

    events_stream = (raw_stream
                     .select(F.from_json(F.col("value").cast("string"),
                                         event_schema).alias("e"))
                     .select("e.*")
                     .withWatermark("timestamp", "30 seconds"))

    r = redis_lib.Redis.from_url(REDIS_URL)

    def _load_factors_and_sim() -> tuple[dict, dict, str | None]:
        """Load item factors + sim maps from Redis; return (factors, sim, version)."""
        mids = [int(k.decode().split(":")[1]) for k in r.scan_iter("item_factor:*")]
        factors = load_item_factors(r, mids)
        sids = [int(k.decode().split(":")[1]) for k in r.scan_iter("item_sim:*")]
        sims = load_item_sim(r, sids)
        ver_raw = r.get("model:version")
        version = ver_raw.decode() if ver_raw else None
        print(f"Loaded {len(factors)} item factors, {len(sims)} item sims (model:version={version})")
        return factors, sims, version

    all_item_factors, all_item_sim, current_model_version = _load_factors_and_sim()

    # Load movie metadata (for item_sim expansion and genre preferences)
    pg_meta = psycopg2.connect(POSTGRES_URL)
    with pg_meta.cursor() as cur:
        cur.execute("SELECT movie_id, title, genres FROM movies")
        all_movie_meta = {
            row[0]: {"movie_id": row[0], "title": row[1], "genres": row[2]}
            for row in cur.fetchall()
        }
    pg_meta.close()
    print(f"Loaded {len(all_movie_meta)} movie metadata records from PostgreSQL")

    # Use lists so batch_fn can rebind references via closure
    pg_conn_ref = [psycopg2.connect(POSTGRES_URL)]
    model_version_ref = [current_model_version]
    MODEL_RELOAD_EVERY = 60  # batches (~10 min at 10s intervals)

    def batch_fn(batch_df, batch_id: int) -> None:
        pg_conn_ref[0] = _get_pg_conn(POSTGRES_URL, pg_conn_ref[0])

        # Hot-reload item factors when bootstrap writes a new model:version
        if batch_id % MODEL_RELOAD_EVERY == 0:
            ver_raw = r.get("model:version")
            new_version = ver_raw.decode() if ver_raw else None
            if new_version and new_version != model_version_ref[0]:
                print(f"Batch {batch_id}: model version changed "
                      f"{model_version_ref[0]} → {new_version}, hot-reloading factors")
                new_factors, new_sims, _ = _load_factors_and_sim()
                all_item_factors.clear()
                all_item_factors.update(new_factors)
                all_item_sim.clear()
                all_item_sim.update(new_sims)
                model_version_ref[0] = new_version

        process_batch(batch_df, batch_id, r, pg_conn_ref[0],
                      all_item_factors, all_item_sim, all_movie_meta)

    query = (events_stream.writeStream
             .foreachBatch(batch_fn)
             .trigger(processingTime="10 seconds")
             .option("checkpointLocation", CHECKPOINT_DIR)
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
