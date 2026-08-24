import { useState } from 'react'
import { Heart, Eye, Star, Film } from 'lucide-react'
import { Rec, EventType } from '../types'
import { formatTitle, extractYear } from '../utils/title'
import { normalizeScore } from '../utils/score'
import { Tooltip } from './Tooltip'

interface Props {
  rec:         Rec
  index:       number
  onFireEvent: (eventType: EventType, rating?: number) => void
  onCardClick: () => void
}

const GENRE_GRADIENTS: Record<string, string> = {
  Action:    'from-red-900/80 via-orange-950/60 to-navy-800',
  Adventure: 'from-emerald-900/80 via-teal-950/60 to-navy-800',
  Animation: 'from-sky-800/80 via-blue-950/60 to-navy-800',
  Comedy:    'from-yellow-800/80 via-amber-950/60 to-navy-800',
  Drama:     'from-violet-900/80 via-purple-950/60 to-navy-800',
  Horror:    'from-red-950/80 via-rose-950/60 to-navy-800',
  Romance:   'from-rose-800/80 via-pink-950/60 to-navy-800',
  'Sci-Fi':  'from-cyan-900/80 via-sky-950/60 to-navy-800',
  Thriller:  'from-gray-800/80 via-zinc-950/60 to-navy-800',
  Crime:     'from-slate-700/80 via-gray-950/60 to-navy-800',
}
const DEFAULT_GRADIENT = 'from-indigo-900/80 via-slate-950/60 to-navy-800'

export function MovieCard({ rec, index, onFireEvent, onCardClick }: Props) {
  const [hoveredStar, setHoveredStar] = useState(0)
  const [sentState, setSentState]     = useState<string | null>(null)

  const gradient   = GENRE_GRADIENTS[(rec.genres ?? '').split('|')[0]] ?? DEFAULT_GRADIENT
  const genres     = (rec.genres ?? '').split('|').filter(Boolean).slice(0, 2)
  const year       = extractYear(rec.title)
  const cleanTitle = formatTitle(rec.title)
  const hasScore   = rec.score != null
  const score      = rec.score_pct ?? normalizeScore(rec.score ?? 0)

  const handleStar = (n: number, e: React.MouseEvent) => {
    e.stopPropagation()
    onFireEvent('rating', n)
    setSentState(`★${n}`)
    setTimeout(() => setSentState(null), 1400)
  }
  const handleAction = (type: EventType, e: React.MouseEvent) => {
    e.stopPropagation()
    onFireEvent(type)
    setSentState(type)
    setTimeout(() => setSentState(null), 1400)
  }

  return (
    <div
      onClick={onCardClick}
      data-testid={`movie-card-${rec.movie_id}`}
      className="group relative flex flex-col overflow-hidden rounded-xl border border-navy-600 bg-navy-800 opacity-0 shadow-lg animate-fade-slide-up transition-all duration-300 hover:-translate-y-1 hover:border-accent/40 hover:shadow-2xl hover:shadow-black/50 cursor-pointer"
      style={{ animationDelay: `${(index % 12) * 50}ms`, animationFillMode: 'forwards' }}
    >
      {/* Poster / gradient media */}
      <div className={`relative aspect-[2/3] w-full overflow-hidden bg-gradient-to-br ${gradient}`}>
        {rec.poster ? (
          <img
            src={rec.poster}
            alt={cleanTitle}
            loading="lazy"
            className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-105"
          />
        ) : (
          <div className="flex h-full w-full items-center justify-center">
            <Film className="text-slate-500" size={28} />
          </div>
        )}

        {hasScore && (
          <div
            className="absolute right-2 top-2 flex h-9 w-9 items-center justify-center rounded-full bg-accent text-xs font-bold text-white shadow-lg"
            title="Relevance score (0–99)"
            data-testid={`score-badge-${rec.movie_id}`}
          >
            {score}
          </div>
        )}

        {/* Hover overlay with actions */}
        <div
          className="absolute inset-x-0 bottom-0 translate-y-full bg-black/70 backdrop-blur-sm p-3 transition-transform duration-200 group-hover:translate-y-0"
          onClick={(e) => e.stopPropagation()}
        >
          {sentState ? (
            <div className="py-1 text-center text-sm font-semibold text-emerald-400">
              ✓ {sentState} sent
            </div>
          ) : (
            <>
              <div className="mb-2 flex justify-center gap-0.5">
                {[1, 2, 3, 4, 5].map((n) => (
                  <button
                    key={n}
                    data-testid={`rate-star-${rec.movie_id}-${n}`}
                    onMouseEnter={() => setHoveredStar(n)}
                    onMouseLeave={() => setHoveredStar(0)}
                    onClick={(e) => handleStar(n, e)}
                    className="p-0.5"
                  >
                    <Star size={17} className={n <= hoveredStar ? 'fill-gold text-gold' : 'text-slate-500'} />
                  </button>
                ))}
              </div>
              <div className="flex justify-center gap-2">
                <Tooltip label="Like this movie">
                  <button
                    onClick={(e) => handleAction('like', e)}
                    data-testid={`like-button-${rec.movie_id}`}
                    className="flex h-8 w-8 items-center justify-center rounded-full bg-accent/90 text-white transition-transform hover:scale-110"
                  >
                    <Heart size={15} />
                  </button>
                </Tooltip>
                <Tooltip label="Mark as viewed">
                  <button
                    onClick={(e) => handleAction('view', e)}
                    data-testid={`view-button-${rec.movie_id}`}
                    className="flex h-8 w-8 items-center justify-center rounded-full bg-navy-600 text-slate-100 transition-transform hover:scale-110"
                  >
                    <Eye size={15} />
                  </button>
                </Tooltip>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Caption */}
      <div className="p-2.5">
        <h3 className="truncate text-sm font-semibold text-white">{cleanTitle}</h3>
        <div className="mt-1 flex flex-wrap items-center gap-1.5">
          {year && <span className="text-xs text-slate-400">{year}</span>}
          {genres.map((g) => (
            <span key={g} className="rounded-full bg-white/10 px-2 py-0.5 text-[11px] text-slate-300">
              {g}
            </span>
          ))}
        </div>
      </div>
    </div>
  )
}
