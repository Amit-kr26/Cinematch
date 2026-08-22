import { useState, useEffect, useRef, useCallback } from 'react'
import axios from 'axios'
import { useRecommendations } from './hooks/useRecommendations'
import { useStats } from './hooks/useStats'
import { useMovies } from './hooks/useMovies'
import { usePipelineAnimation } from './hooks/usePipelineAnimation'
import { RecommendationList } from './components/RecommendationList'
import { MovieGrid } from './components/MovieGrid'
import { StatusBar } from './components/StatusBar'
import { PipelineBar } from './components/PipelineBar'
import { TopNav } from './components/TopNav'
import { Sidebar } from './components/Sidebar'
import { MovieModal } from './components/MovieModal'
import { WelcomeModal } from './components/WelcomeModal'
import { ErrorBoundary } from './components/ErrorBoundary'
import { Rec, Movie, EventType, SessionEvent } from './types'

export default function App() {
  // One random MovieLens viewer is assigned per session — no user picker.
  const [activeId] = useState(() => Math.floor(Math.random() * 6040) + 1)
  const [showWelcome, setShowWelcome] = useState(true)
  const [genres, setGenres]           = useState<string[]>([])
  const [search, setSearch]           = useState('')
  const [debouncedSearch, setDebounced] = useState('')
  const [genre, setGenre]             = useState('All')
  const [sessionEvents, setSessionEvents] = useState<SessionEvent[]>([])
  const [selectedRec, setSelectedRec] = useState<Rec | null>(null)

  const { data, loading, error, wsConnected, refetch } = useRecommendations(activeId)
  const { stats, refresh: refreshStats } = useStats()
  const { stage, sparkCountdown, fireEvent, notifyRedisUpdated } = usePipelineAnimation()
  const { movies, total, hasMore, loading: browseLoading, loadingMore, loadMore } =
    useMovies(debouncedSearch, genre)

  const scrollRef = useRef<HTMLElement | null>(null)
  const filtering = debouncedSearch.trim() !== '' || genre !== 'All'

  useEffect(() => {
    axios.get<{ genres: string[] }>('/genres').then((r) => setGenres(r.data?.genres ?? [])).catch(() => {})
  }, [])

  useEffect(() => {
    const t = setTimeout(() => setDebounced(search), 350)
    return () => clearTimeout(t)
  }, [search])

  // Animate the Kafka→Spark→Redis pipeline when fresh recs land after an event
  const pendingPipeline = useRef(false)
  useEffect(() => {
    if (pendingPipeline.current && data) {
      pendingPipeline.current = false
      notifyRedisUpdated()
    }
  }, [data, notifyRedisUpdated])

  const handleFireEvent = useCallback(async (
    rec: Rec | Movie,
    eventType: EventType,
    rating?: number,
  ) => {
    fireEvent()
    await axios.post('/simulate', {
      user_id:    activeId,
      movie_id:   rec.movie_id,
      event_type: eventType,
      rating,
    })
    setSessionEvents(prev => [...prev, {
      movieId:   rec.movie_id,
      title:     rec.title,
      eventType,
      rating,
      firedAt:   new Date(),
    }])
    pendingPipeline.current = true
    refreshStats()                       // events counter updates instantly
    setTimeout(() => { refetch(); refreshStats() }, 3000)
  }, [activeId, fireEvent, refetch, refreshStats])

  const handleHome = () => {
    setSearch('')
    setDebounced('')
    setGenre('All')
    if (scrollRef.current) scrollRef.current.scrollTop = 0
  }

  const onScroll = (e: React.UIEvent<HTMLElement>) => {
    const el = e.currentTarget
    if (el.scrollHeight - el.scrollTop - el.clientHeight < 500) loadMore()
  }

  return (
    <div className="min-h-screen h-screen bg-navy-950 text-white flex flex-col">
      <StatusBar source={data?.source ?? null} stats={stats}
                 variant={data?.variant ?? null} wsConnected={wsConnected} />
      <PipelineBar stage={stage} sparkCountdown={sparkCountdown} />
      <TopNav
        search={search}
        onSearch={setSearch}
        genre={genre}
        genres={genres}
        onGenre={setGenre}
        onHome={handleHome}
        userId={activeId}
      />

      <div className="flex flex-1 min-h-0">
        <main ref={scrollRef} onScroll={onScroll} className="flex-1 min-w-0 px-6 py-5 overflow-y-auto">
          {!filtering && (
            <section className="mb-10">
              <RecommendationList
                data={data}
                loading={loading}
                error={error}
                onFireEvent={handleFireEvent}
                onCardClick={setSelectedRec}
              />
            </section>
          )}

          <section>
            <div className="flex items-baseline gap-3 mb-5">
              <h2 className="text-xl font-semibold text-slate-200">
                {filtering
                  ? debouncedSearch
                    ? `Results for “${debouncedSearch}”`
                    : `${genre} movies`
                  : 'Browse all movies'}
              </h2>
              {total > 0 && <span className="text-sm text-slate-500">{total.toLocaleString()} titles</span>}
            </div>

            {!browseLoading && movies.length === 0 ? (
              <div className="rounded-xl border border-navy-600 bg-navy-800/50 p-10 text-center text-slate-400">
                No movies found. Try a different search or genre.
              </div>
            ) : (
              <MovieGrid
                movies={movies}
                loading={browseLoading}
                skeletonCount={20}
                onFireEvent={handleFireEvent}
                onCardClick={setSelectedRec}
              />
            )}

            {loadingMore && (
              <div className="mt-6 text-center text-slate-400 text-sm animate-pulse">Loading more…</div>
            )}
            {hasMore && !loadingMore && (
              <div className="mt-6 flex justify-center">
                <button
                  onClick={loadMore}
                  data-testid="load-more-button"
                  className="rounded-lg border border-navy-600 bg-navy-800 px-6 py-2.5 text-sm font-medium text-slate-200 transition-colors hover:border-accent/50"
                >
                  Load more
                </button>
              </div>
            )}
          </section>
        </main>

        <Sidebar sessionEvents={sessionEvents} />
      </div>

      {showWelcome && (
        <WelcomeModal userId={activeId} onClose={() => setShowWelcome(false)} />
      )}

      {selectedRec && (
        <ErrorBoundary key={selectedRec.movie_id} onDismiss={() => setSelectedRec(null)}>
          <MovieModal
            rec={selectedRec}
            onClose={() => setSelectedRec(null)}
            onFireEvent={handleFireEvent}
          />
        </ErrorBoundary>
      )}
    </div>
  )
}
