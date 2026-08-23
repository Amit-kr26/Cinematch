import { useState, useEffect } from 'react'
import { RecsResponse, Rec, EventType } from '../types'
import { MovieCard } from './MovieCard'

interface Props {
  data:        RecsResponse | null
  loading:     boolean
  error:       string | null
  onFireEvent: (rec: Rec, eventType: EventType, rating?: number) => void
  onCardClick: (rec: Rec) => void
}

function SkeletonCard() {
  return <div className="skeleton min-h-[220px] rounded-xl" />
}

function formatUpdatedAgo(updatedAt: string): string {
  const diffSec = Math.floor((Date.now() - new Date(updatedAt).getTime()) / 1000)
  if (diffSec < 60) return 'Updated just now'
  if (diffSec < 3600) return `Updated ${Math.floor(diffSec / 60)}m ago`
  return `Updated ${Math.floor(diffSec / 3600)}h ago`
}

export function RecommendationList({ data, loading, error, onFireEvent, onCardClick }: Props) {
  const [, setTick] = useState(0)
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 30_000)
    return () => clearInterval(id)
  }, [])

  if (error) {
    return (
      <div className="rounded-xl border border-red-500/30 bg-red-500/10 p-5">
        <p className="text-red-400 text-base font-medium">Error: {error}</p>
      </div>
    )
  }

  if (loading && !data) {
    return (
      <div>
        <div className="flex items-center gap-3 mb-5">
          <h2 className="text-xl font-semibold text-slate-200">Top Picks</h2>
          <div className="w-2.5 h-2.5 rounded-full bg-slate-500 animate-pulse" />
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
          {Array.from({ length: 10 }).map((_, i) => <SkeletonCard key={i} />)}
        </div>
      </div>
    )
  }

  if (!data) return null

  if (data.source === 'empty') {
    return (
      <div className="rounded-xl border border-navy-600 bg-navy-800/50 p-10 text-center mt-5">
        <p className="text-slate-300 text-base font-medium mb-1.5">No recommendations yet for this user</p>
        <p className="text-slate-500 text-sm">
          Hover any card and click a star or action button to fire an event. Spark re-ranks in ~10s.
        </p>
      </div>
    )
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <h2 className="text-xl font-semibold text-slate-200">Top Picks</h2>
        {loading && <div className="w-2.5 h-2.5 rounded-full bg-accent animate-pulse" title="Refreshing" />}
      </div>

      {data.updated_at && (
        <p className="text-slate-500 text-sm mb-5">{formatUpdatedAgo(data.updated_at)}</p>
      )}

      {data.variant === 'control' ? (
        <div className="mb-5 rounded-lg border border-purple-500/30 bg-purple-500/5 px-5 py-3.5">
          <p className="text-purple-300 text-sm font-medium">
            Experiment &middot; Control group — you are seeing the static ALS model ranking.
            Your events are recorded for measurement but do not reorder this list.
          </p>
        </div>
      ) : data.source === 'redis' || data.source === 'postgres' ? (
        <div className="mb-5 rounded-lg border border-emerald-500/30 bg-emerald-500/5 px-5 py-3.5">
          <p className="text-emerald-300 text-sm font-medium">
            Experiment &middot; Treatment group — this order was re-ranked from your events
            (Kafka &rarr; Spark, ~10 s cycles).
          </p>
        </div>
      ) : (
        <div className="mb-5 rounded-lg border border-amber-500/30 bg-amber-500/5 px-5 py-3.5 flex items-start gap-3">
          <svg className="w-5 h-5 text-amber-400 shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
              d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <p className="text-amber-300 text-sm font-medium">
            Experiment &middot; Treatment group on the static ALS baseline — nothing re-ranked
            yet. Hover a card and rate or view to trigger live re-ranking via Kafka &rarr; Spark.
          </p>
        </div>
      )}

      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
        {data.recs.map((rec, i) => (
          <MovieCard
            key={rec.movie_id}
            rec={rec}
            index={i}
            onFireEvent={(type, rating) => { Promise.resolve(onFireEvent(rec, type, rating)).catch(() => {}) }}
            onCardClick={() => onCardClick(rec)}
          />
        ))}
      </div>
    </div>
  )
}
