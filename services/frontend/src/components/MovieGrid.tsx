import { Movie, EventType } from '../types'
import { MovieCard } from './MovieCard'

interface Props {
  movies:      Movie[]
  loading?:    boolean
  skeletonCount?: number
  onFireEvent: (movie: Movie, eventType: EventType, rating?: number) => Promise<void> | void
  onCardClick: (movie: Movie) => void
}

function SkeletonCard() {
  return (
    <div>
      <div className="skeleton aspect-[2/3] w-full rounded-xl" />
      <div className="skeleton mt-2 h-3 w-3/4 rounded" />
    </div>
  )
}

export function MovieGrid({ movies, loading, skeletonCount = 12, onFireEvent, onCardClick }: Props) {
  return (
    <div
      className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5"
      data-testid="movie-grid"
    >
      {loading && movies.length === 0
        ? Array.from({ length: skeletonCount }).map((_, i) => <SkeletonCard key={i} />)
        : movies.map((m, i) => (
            <MovieCard
              key={m.movie_id}
              rec={m}
              index={i}
              onFireEvent={(type, rating) => onFireEvent(m, type, rating)}
              onCardClick={() => onCardClick(m)}
            />
          ))}
    </div>
  )
}
