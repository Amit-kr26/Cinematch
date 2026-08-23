import { Film, Wifi, RefreshCw } from 'lucide-react'
import { StatsData } from '../types'

interface Props {
  source:      'redis' | 'postgres' | 'als_baseline' | 'cold_start_popular' | 'empty' | null
  stats:       StatsData | null
  variant:     'control' | 'treatment' | null
  wsConnected: boolean
  mode?:       'full' | 'lite' | null
}

const sourceLabel: Record<string, { label: string; color: string }> = {
  redis:              { label: 'Redis LIVE',   color: 'text-emerald-400 bg-emerald-900/30 border-emerald-700/40' },
  postgres:           { label: 'Postgres',     color: 'text-blue-400 bg-blue-900/30 border-blue-700/40' },
  als_baseline:       { label: 'ALS Baseline', color: 'text-gold bg-amber-900/30 border-amber-700/40' },
  cold_start_popular: { label: 'Trending',     color: 'text-rose-400 bg-rose-900/30 border-rose-700/40' },
  empty:              { label: 'No recs',      color: 'text-slate-400 bg-slate-800/30 border-slate-700/40' },
}

const AB_LABEL = {
  control:   { short: 'A/B · Control — static ALS',   tip: 'Control group: you always see the static ALS model ranking; events are measured, not applied.' },
  treatment: { short: 'A/B · Treatment — personalized', tip: 'Treatment group: your list is re-ranked from your events via Kafka → Spark.' },
} as const

function safePct(v: number | undefined | null): number {
  if (v == null || !Number.isFinite(v) || Number.isNaN(v)) return 0
  return Math.round(v * 100)
}

export function StatusBar({ source, stats, variant, wsConnected, mode }: Props) {
  const src = source ? sourceLabel[source] : null

  return (
    <header className="bg-navy-900/80 backdrop-blur-xl border-b border-navy-700 px-6 py-3.5 flex items-center justify-between">
      <div className="flex items-center gap-3">
        <Film className="text-accent" size={24} style={{ filter: 'drop-shadow(0 0 12px rgba(229,9,20,0.5))' }} />
        <h1 className="font-display text-2xl font-extrabold tracking-tight text-white">CineMatch</h1>
        <span className="text-slate-500 text-sm hidden md:block border-l border-navy-700 pl-3">
          Real-time ML Recommendation Engine
        </span>
      </div>

      <div className="flex items-center gap-2.5">
        {src && (
          <span className={`flex items-center gap-2 px-3.5 py-1.5 rounded-full text-sm font-medium border transition-all duration-500 ${src.color}`}>
            <span className="w-2 h-2 rounded-full bg-current animate-pulse" />
            {src.label}
          </span>
        )}
        {stats && stats.p50_latency_ms > 0 && (
          <span className="px-3.5 py-1.5 rounded-full text-sm font-medium border text-blue-400 bg-blue-900/20 border-blue-700/30 transition-all duration-500">
            {stats.p50_latency_ms.toFixed(1)}ms p50
          </span>
        )}
        {stats && (
          <span className="px-3.5 py-1.5 rounded-full text-sm font-medium border text-gold bg-amber-900/20 border-amber-700/30 transition-all duration-500">
            {safePct(stats.cache_hit_rate)}% cache
          </span>
        )}
        {stats && stats.total_events > 0 && (
          <span className="px-3.5 py-1.5 rounded-full text-sm font-medium border text-violet-400 bg-violet-900/20 border-violet-700/30 transition-all duration-500">
            {stats.total_events} events
          </span>
        )}
        {variant && mode !== 'lite' && (
          <span
            title={AB_LABEL[variant].tip}
            className="px-3.5 py-1.5 rounded-full text-sm font-medium border text-purple-400 bg-purple-900/20 border-purple-700/30 transition-all duration-500 hidden lg:inline-flex cursor-help"
          >
            {AB_LABEL[variant].short}
          </span>
        )}
        <span className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-full text-sm font-medium border transition-all duration-500 ${
          wsConnected
            ? 'text-emerald-400 bg-emerald-900/20 border-emerald-700/30'
            : 'text-slate-500 bg-slate-800/20 border-slate-700/30'
        }`}>
          {wsConnected ? <Wifi size={13} /> : <RefreshCw size={13} />}
          {wsConnected ? 'WS Live' : 'Polling'}
        </span>
      </div>
    </header>
  )
}
