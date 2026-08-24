export interface Rec {
  movie_id: number
  title:    string
  genres:   string
  score?:   number
  score_pct?: number | null
  // TMDB enrichment (optional)
  poster?:      string | null
  backdrop?:    string | null
  overview?:    string
  year?:        number | null
  tmdb_rating?: number
}

export type Movie = Rec

export interface RecsResponse {
  user_id:    number
  recs:       Rec[]
  source:     'redis' | 'postgres' | 'als_baseline' | 'cold_start_popular' | 'empty'
  updated_at: string
  variant:    'control' | 'treatment'
}

export interface MoviesResponse {
  movies:    Movie[]
  page:      number
  page_size: number
  total:     number
  has_more:  boolean
}

export type EventType = 'click' | 'view' | 'like' | 'rating'

export type PipelineStage =
  | 'idle'
  | 'event_sent'
  | 'kafka_ingesting'
  | 'spark_processing'
  | 'redis_updated'
  | 'complete'

export interface SessionEvent {
  movieId:   number
  title:     string
  eventType: EventType
  rating?:   number
  firedAt:   Date
}

export interface StatsData {
  cache_hit_rate: number
  p50_latency_ms: number
  total_events:   number
}

export interface SimilarMovie {
  movie_id:   number
  title:      string
  genres?:    string
  year?:      number | null
  poster?:    string | null
  similarity: number
}
