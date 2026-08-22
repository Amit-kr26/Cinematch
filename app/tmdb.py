"""Optional live TMDB art enrichment for the lite app.

Activated only when the TMDB_API_KEY env var is set (add it as a HuggingFace
Space secret). Responses enqueue movie_ids whose posters are missing; a
background task searches TMDB by cleaned title (+year), persists results into
the movies table and refreshes the in-memory cache. Without a key everything
is a no-op and the bundled seed art is used as-is.
"""
import asyncio
import logging
import os
import re
import sqlite3

log = logging.getLogger("tmdb")

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_SEARCH  = "https://api.themoviedb.org/3/search/movie"
IMG_BASE     = "https://image.tmdb.org/t/p/"

YEAR_RE    = re.compile(r"\((\d{4})\)\s*$")
ARTICLE_RE = re.compile(
    r"^(.*),\s+(The|A|An|Les|La|Le|Los|Las|Un|Une|Der|Die|Das|Il|Lo)$", re.I)

MAX_CONCURRENCY = 4

_pending: set[int] = set()
_client = None
_semaphore = None


def active() -> bool:
    return bool(TMDB_API_KEY)


def init() -> None:
    """Create the shared HTTP client if a key is configured."""
    global _client, _semaphore
    if active():
        import httpx
        _client = httpx.AsyncClient(timeout=10)
        _semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        log.info("Live TMDB enrichment enabled")


async def close() -> None:
    if _client is not None:
        await _client.aclose()


def clean_title(raw: str | None) -> str:
    stripped = YEAR_RE.sub("", raw or "").strip()
    m = ARTICLE_RE.match(stripped)
    return f"{m.group(2)} {m.group(1)}" if m else stripped


def extract_year(raw: str | None) -> int | None:
    m = YEAR_RE.search(raw or "")
    return int(m.group(1)) if m else None


def request_enrichment(movie_ids: list[int]) -> None:
    """Fire-and-forget: schedule background lookups for ids lacking art."""
    if not active():
        return
    todo = [mid for mid in movie_ids if mid not in _pending]
    _pending.update(todo)
    if todo:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_enrich_batch(todo))
        except RuntimeError:
            _pending.difference_update(todo)


async def _enrich_batch(ids: list[int]) -> None:
    db_path = __import__("os").getenv("DB_PATH", "/data/app.db")
    try:
        await asyncio.gather(*[_fetch_and_store(mid, db_path) for mid in ids])
    finally:
        _pending.difference_update(ids)


async def _fetch_one(title: str, year: int | None) -> dict | None:
    assert _client is not None
    params = {"api_key": TMDB_API_KEY, "query": clean_title(title)}
    if year:
        params["year"] = year
    for attempt in range(3):
        try:
            r = await _client.get(TMDB_SEARCH, params=params)
            if r.status_code == 429:
                await asyncio.sleep(2 ** attempt)
                continue
            results = r.json().get("results", []) if r.status_code == 200 else []
            if not results and year:
                params.pop("year", None)
                r = await _client.get(TMDB_SEARCH, params=params)
                results = r.json().get("results", []) if r.status_code == 200 else []
            best = results[0] if results else {}
            if not best:
                return None
            return {
                "poster":   IMG_BASE + "w342" + best["poster_path"]
                            if best.get("poster_path") else None,
                "backdrop": IMG_BASE + "w780" + best["backdrop_path"]
                            if best.get("backdrop_path") else None,
                "overview": best.get("overview") or "",
                "tmdb_rating": best.get("vote_average") or 0.0,
            }
        except Exception as e:
            if attempt == 2:
                log.warning("TMDB lookup failed for %r: %s", title, e)
                return None
            await asyncio.sleep(0.5 * (attempt + 1))
    return None


async def _fetch_and_store(movie_id: int, db_path: str) -> None:
    from app.main import _movies
    meta = _movies.get(movie_id)
    if not meta:
        return
    async with _semaphore:
        data = await _fetch_one(meta.get("title"), meta.get("year"))
    if not data:
        return
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE movies SET poster=?, backdrop=?, overview=?, tmdb_rating=? "
            "WHERE movie_id=?",
            (data["poster"], data["backdrop"], data["overview"],
             data["tmdb_rating"], movie_id))
        conn.commit()
    finally:
        conn.close()
    meta.update(data)
