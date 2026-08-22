import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { X, Heart, Eye, Star, Film } from 'lucide-react'
import { Rec, EventType, SimilarMovie } from '../types'
import { formatTitle, extractYear } from '../utils/title'
import { Tooltip } from './Tooltip'

interface Props {
  rec:         Rec
  onClose:     () => void
  onFireEvent: (movie: Rec, eventType: EventType, rating?: number) => Promise<void>
}

export function MovieModal({ rec, onClose, onFireEvent }: Props) {
  const [movie, setMovie]     = useState<Rec>(rec)
  const [similar, setSimilar] = useState<SimilarMovie[]>([])
  const [hoveredStar, setHovered] = useState(0)
  const [firing, setFiring]   = useState(false)
  const [fired, setFired]     = useState<string | null>(null)
  const [error, setError]     = useState<string | null>(null)

  useEffect(() => {
    if (!movie?.movie_id) return
    let cancelled = false
    axios
      .get<{ movie: Rec; similar: SimilarMovie[] }>(`/movies/${movie.movie_id}`)
      .then((r) => {
        if (cancelled || !r.data?.movie) return
        setMovie(r.data.movie)
        setSimilar(r.data.similar ?? [])
      })
      .catch(() => {})
    return () => { cancelled = true }
  }, [movie.movie_id])

  const handleFire = useCallback(async (type: EventType, rating?: number) => {
    setFiring(true)
    setError(null)
    try {
      await onFireEvent(movie, type, rating)
      setFired(type === 'rating' ? `★${rating} sent` : `${type} sent`)
      setTimeout(onClose, 800)
    } catch {
      setError('Could not send event — service unavailable or rate limited.')
    } finally {
      setFiring(false)
    }
  }, [onFireEvent, onClose, movie])

  const openSimilar = (s: SimilarMovie) => {
    setFired(null)
    setError(null)
    setHovered(0)
    setSimilar([])
    setMovie({ ...s } as Rec)
  }

  const genres = (movie.genres ?? '').split('|').filter(Boolean).slice(0, 4)
  const year   = movie.year ?? extractYear(movie.title)

  return (
    <div
      className="fixed inset-0 z-[90] flex items-center justify-center bg-black/75 backdrop-blur-sm p-4"
      onClick={onClose}
      data-testid="movie-modal"
    >
      <div
        className="relative w-full max-w-3xl overflow-hidden rounded-2xl border border-navy-600 bg-navy-900 shadow-2xl animate-scale-in"
        onClick={(e) => e.stopPropagation()}
      >
        {movie.backdrop && (
          <div className="absolute inset-x-0 top-0 h-44 overflow-hidden">
            <img src={movie.backdrop} alt="" className="h-full w-full object-cover opacity-30" />
            <div className="absolute inset-0 bg-gradient-to-t from-navy-900 to-transparent" />
          </div>
        )}

        <button
          onClick={onClose}
          data-testid="modal-close-button"
          className="absolute right-3 top-3 z-10 flex h-9 w-9 items-center justify-center rounded-full bg-black/60 text-slate-300 hover:text-white"
        >
          <X size={18} />
        </button>

        <div className="relative flex flex-col gap-5 p-6 sm:flex-row sm:p-8">
          <div className="mx-auto w-40 shrink-0 sm:mx-0">
            <div className="aspect-[2/3] overflow-hidden rounded-xl border border-navy-600 bg-navy-800">
              {movie.poster ? (
                <img src={movie.poster} alt={formatTitle(movie.title)} className="h-full w-full object-cover" />
              ) : (
                <div className="flex h-full items-center justify-center">
                  <Film className="text-slate-600" size={30} />
                </div>
              )}
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <h2 className="font-display text-2xl font-bold text-white">{formatTitle(movie.title)}</h2>
            <div className="mt-2 flex flex-wrap items-center gap-2 text-sm text-slate-400">
              {year && <span>{year}</span>}
              {!!movie.tmdb_rating && movie.tmdb_rating > 0 && (
                <span className="flex items-center gap-1 text-gold">
                  <Star size={13} className="fill-gold" /> {movie.tmdb_rating.toFixed(1)}
                </span>
              )}
            </div>
            <div className="mt-2 flex flex-wrap gap-1.5">
              {genres.map((g) => (
                <span key={g} className="rounded-full border border-navy-600 bg-black/30 px-2.5 py-0.5 text-xs text-slate-300">
                  {g}
                </span>
              ))}
            </div>

            <p className="mt-4 text-sm leading-relaxed text-slate-300 line-clamp-5">
              {movie.overview || 'No description available for this title.'}
            </p>

            <div className="mt-5">
              {fired ? (
                <div className="text-sm font-semibold text-emerald-400">✓ {fired}</div>
              ) : (
                <>
                <div className="flex flex-wrap items-center gap-3">
                  <div className="flex items-center gap-1">
                    {[1, 2, 3, 4, 5].map((n) => (
                      <button
                        key={n}
                        disabled={firing}
                        data-testid={`modal-rate-star-${n}`}
                        onMouseEnter={() => setHovered(n)}
                        onMouseLeave={() => setHovered(0)}
                        onClick={() => handleFire('rating', n)}
                      >
                        <Star size={22} className={n <= hoveredStar ? 'fill-gold text-gold' : 'text-slate-600'} />
                      </button>
                    ))}
                  </div>
                  <Tooltip label="Like this movie">
                    <button
                      disabled={firing}
                      onClick={() => handleFire('like')}
                      data-testid="modal-like-button"
                      className="flex items-center gap-2 rounded-lg bg-accent px-3.5 py-2 text-sm font-semibold text-white hover:bg-accent-light disabled:opacity-50"
                    >
                      <Heart size={15} /> Like
                    </button>
                  </Tooltip>
                  <Tooltip label="Mark as viewed">
                    <button
                      disabled={firing}
                      onClick={() => handleFire('view')}
                      data-testid="modal-view-button"
                      className="flex items-center gap-2 rounded-lg border border-navy-600 bg-navy-700 px-3.5 py-2 text-sm font-medium text-slate-200 hover:bg-navy-600 disabled:opacity-50"
                    >
                      <Eye size={15} /> Viewed
                    </button>
                  </Tooltip>
                </div>
                {error && (
                  <div className="mt-2 text-xs font-medium text-red-400">{error}</div>
                )}
                </>
              )}
            </div>
          </div>
        </div>

        {similar.length > 0 && (
          <div className="border-t border-navy-700 px-6 py-4 sm:px-8">
            <div className="mb-3 text-xs uppercase tracking-[0.2em] text-slate-500">More like this</div>
            <div className="grid grid-cols-3 gap-3 sm:grid-cols-6">
              {similar.map((s) => (
                <button
                  key={s.movie_id}
                  onClick={() => openSimilar(s)}
                  data-testid={`similar-movie-${s.movie_id}`}
                  className="group text-left"
                >
                  <div className="aspect-[2/3] overflow-hidden rounded-lg border border-navy-600 bg-navy-800">
                    {s.poster ? (
                      <img src={s.poster} alt={formatTitle(s.title)} className="h-full w-full object-cover transition group-hover:opacity-80" />
                    ) : (
                      <div className="flex h-full items-center justify-center p-1 text-center text-[10px] text-slate-400">
                        {formatTitle(s.title)}
                      </div>
                    )}
                  </div>
                  <div className="mt-1 truncate text-xs text-slate-400">{formatTitle(s.title)}</div>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
