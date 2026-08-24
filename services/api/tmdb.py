"""TMDB enrichment for the API service.

Searches TMDB by (clean) title + year, returns poster/backdrop/overview/rating,
and caches the result in Redis under `tmdb:{movie_id}` so repeat lookups are free.
Degrades gracefully: if TMDB_API_KEY is unset or the HTTP client is unavailable,
items are returned with empty TMDB fields rather than raising.
"""
import asyncio
import json
import os
import re

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_SEARCH  = "https://api.themoviedb.org/3/search/movie"
IMG_BASE     = "https://image.tmdb.org/t/p/"
CACHE_TTL    = 60 * 60 * 24 * 30  # 30 days

YEAR_RE    = re.compile(r"\((\d{4})\)\s*$")
ARTICLE_RE = re.compile(
    r"^(.*),\s+(The|A|An|Les|La|Le|Los|Las|Un|Une|Der|Die|Das|Il|Lo)$", re.I
)

EMPTY = {"poster": None, "backdrop": None, "overview": "", "tmdb_rating": 0.0, "tmdb_id": None}


def clean_title(raw: str | None) -> str:
    """'Wizard of Oz, The (1939)' -> 'The Wizard of Oz' (year stripped)."""
    stripped = YEAR_RE.sub("", raw or "").strip()
    m = ARTICLE_RE.match(stripped)
    return f"{m.group(2)} {m.group(1)}" if m else stripped


def extract_year(raw: str | None):
    m = YEAR_RE.search(raw or "")
    return int(m.group(1)) if m else None


async def _fetch_one(http, item: dict, retries: int = 3) -> dict:
    title = clean_title(item.get("title"))
    year  = item.get("year") or extract_year(item.get("title"))
    params = {"api_key": TMDB_API_KEY, "query": title}
    if year:
        params["year"] = year
    for attempt in range(retries):
        try:
            r = await http.get(TMDB_SEARCH, params=params)
            if r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            results = r.json().get("results", []) if r.status_code == 200 else []
            if not results and year:
                params.pop("year", None)
                r = await http.get(TMDB_SEARCH, params=params)
                results = r.json().get("results", []) if r.status_code == 200 else []
            best = results[0] if results else {}
            return {
                "tmdb_id":     best.get("id"),
                "poster":      IMG_BASE + "w342" + best["poster_path"] if best.get("poster_path") else None,
                "backdrop":    IMG_BASE + "w780" + best["backdrop_path"] if best.get("backdrop_path") else None,
                "overview":    best.get("overview") or "",
                "tmdb_rating": best.get("vote_average") or 0.0,
            }
        except Exception:
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
            else:
                return dict(EMPTY)
    return dict(EMPTY)


async def enrich_bounded(redis, http, items: list[dict], timeout: float = 1.5) -> None:
    """enrich() with a hard latency ceiling for hot-path callers.

    On a cold TMDB cache the fetches would otherwise run inline inside
    /recommend; bounding them keeps the <2 ms serving story honest — worst
    case a response ships without art and the next request finds it cached.
    """
    if not items:
        return
    try:
        await asyncio.wait_for(enrich(redis, http, items), timeout=timeout)
    except asyncio.TimeoutError:
        pass


async def enrich(redis, http, items: list[dict]) -> list[dict]:
    """Mutates `items` in place, attaching TMDB fields. Returns the same list."""
    if not items:
        return items

    for it in items:
        if not it.get("year"):
            it["year"] = extract_year(it.get("title"))

    # 1. cache lookup
    missing: list[dict] = []
    if redis is not None:
        pipe = redis.pipeline()
        for it in items:
            pipe.get(f"tmdb:{it['movie_id']}")
        cached = await pipe.execute()
        for it, raw in zip(items, cached):
            if raw:
                try:
                    it.update(json.loads(raw))
                    continue
                except Exception:
                    pass
            missing.append(it)
    else:
        missing = list(items)

    # 2. nothing to fetch (or no network) -> fill empties and return
    if not missing or http is None or not TMDB_API_KEY:
        for it in missing:
            for k, v in EMPTY.items():
                it.setdefault(k, v)
        return items

    # 3. fetch missing concurrently and write back to cache
    fetched = await asyncio.gather(*[_fetch_one(http, it) for it in missing])
    if redis is not None:
        pipe = redis.pipeline()
        for it, data in zip(missing, fetched):
            it.update(data)
            # Only cache successful lookups — don't persist empty results so
            # they get retried on the next request.
            if data.get("tmdb_id") is not None:
                pipe.set(f"tmdb:{it['movie_id']}", json.dumps(data), ex=CACHE_TTL)
        await pipe.execute()
    else:
        for it, data in zip(missing, fetched):
            it.update(data)
    return items
