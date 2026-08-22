import { Clapperboard, Zap, Flame, Database, Server, Target, ChevronRight, Activity } from 'lucide-react'
import { PipelineStage } from '../types'

interface Props {
  stage:          PipelineStage
  sparkCountdown: number
}

const NODES = [
  { icon: Clapperboard, label: 'Event',   sub: 'click · rate · like', on: 'border-accent bg-accent/15 text-accent',          glow: 'shadow-accent/30' },
  { icon: Zap,          label: 'Kafka',   sub: '3 partitions',        on: 'border-blue-500 bg-blue-900/25 text-blue-400',     glow: 'shadow-blue-500/30' },
  { icon: Flame,        label: 'Spark',   sub: '10s micro-batch',     on: 'border-orange-500 bg-orange-900/25 text-orange-400', glow: 'shadow-orange-500/30' },
  { icon: Database,     label: 'Redis',   sub: 'TTL 5min',            on: 'border-emerald-500 bg-emerald-900/25 text-emerald-400', glow: 'shadow-emerald-500/30' },
  { icon: Server,       label: 'FastAPI', sub: 'fallback chain',      on: 'border-violet-500 bg-violet-900/25 text-violet-400', glow: 'shadow-violet-500/30' },
  { icon: Target,       label: 'You',     sub: 'top-10',              on: 'border-accent bg-accent/15 text-accent',          glow: 'shadow-accent/30' },
]

const REACHED: Record<PipelineStage, number> = {
  idle: -1, event_sent: 0, kafka_ingesting: 1, spark_processing: 2, redis_updated: 5, complete: 5,
}

export function PipelineBar({ stage, sparkCountdown }: Props) {
  const reached = REACHED[stage]
  const running = stage !== 'idle'

  return (
    <div className="bg-navy-950/90 border-b border-navy-700 px-4 sm:px-6 py-2.5">
      <div className="flex items-center gap-4">
        <div className="hidden shrink-0 items-center gap-2 md:flex">
          <Activity size={14} className={running ? 'text-accent animate-pulse' : 'text-slate-600'} />
          <span className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
            Live pipeline
          </span>
        </div>

        <div className="flex flex-1 items-center justify-center gap-1 overflow-x-auto">
          {NODES.map((node, idx) => {
            const Icon = node.icon
            const active  = running && idx <= reached
            const current = running && idx === reached
            const isSpark = node.label === 'Spark'
            return (
              <div key={idx} className="flex items-center">
                <div
                  className={`flex shrink-0 items-center gap-2 rounded-lg border px-3 py-1.5 transition-all duration-300 ${
                    active ? `${node.on} shadow-lg ${node.glow}` : 'border-navy-700 bg-navy-800/50 text-slate-500'
                  } ${current ? 'scale-[1.05]' : ''}`}
                >
                  <Icon size={15} className={current ? 'animate-pulse' : ''} />
                  <div className="leading-tight">
                    <div className={`text-xs font-semibold ${active ? '' : 'text-slate-400'}`}>{node.label}</div>
                    <div className="text-[10px] text-slate-500">
                      {current && isSpark && stage === 'spark_processing'
                        ? (sparkCountdown > 0 ? `processing ${sparkCountdown}s…` : 'flushing…')
                        : node.sub}
                    </div>
                  </div>
                </div>
                {idx < NODES.length - 1 && (
                  <ChevronRight
                    size={14}
                    className={`mx-0.5 shrink-0 transition-colors duration-300 ${
                      running && idx < reached ? 'text-accent' : 'text-navy-600'
                    }`}
                  />
                )}
              </div>
            )
          })}
        </div>

        <div className="hidden w-[88px] shrink-0 md:block" />
      </div>
    </div>
  )
}
