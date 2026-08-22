import { useState, useEffect, useRef } from 'react'
import axios from 'axios'
import { SessionEvent, EventType } from '../types'
import { formatTitle } from '../utils/title'

interface FeedEvent {
  user_id:    number
  movie_id:   number
  event_type: EventType
  rating?:    number
  timestamp:  string
}

interface Props {
  sessionEvents: SessionEvent[]
}

const eventBadge: Record<EventType, string> = {
  rating: 'text-amber-400 bg-amber-900/30',
  like:   'text-rose-400 bg-rose-900/30',
  view:   'text-blue-400 bg-blue-900/30',
  click:  'text-violet-400 bg-violet-900/30',
}

function formatTime(iso: string) {
  try {
    return new Date(iso).toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return '--:--:--'
  }
}

export function Sidebar({ sessionEvents }: Props) {
  const [feed, setFeed]           = useState<FeedEvent[]>([])
  const [toastVisible, setToast]  = useState(false)
  const prevLen                   = useRef(sessionEvents.length)
  const toastTimer                = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    if (sessionEvents.length > prevLen.current) {
      setToast(true)
      if (toastTimer.current) clearTimeout(toastTimer.current)
      toastTimer.current = setTimeout(() => setToast(false), 4000)
    }
    prevLen.current = sessionEvents.length
  }, [sessionEvents.length])

  useEffect(() => {
    let cancelled = false
    const poll = async () => {
      try {
        const r = await axios.get<{ events: FeedEvent[] }>('/events/recent')
        if (!cancelled) setFeed(r.data.events.slice(0, 8))
      } catch { /* best-effort */ }
    }
    poll()
    const id = setInterval(poll, 3000)
    return () => { cancelled = true; clearInterval(id) }
  }, [])

  const lastEvent = sessionEvents[sessionEvents.length - 1]

  return (
    <aside className="w-56 flex-shrink-0 border-l border-navy-700 bg-navy-950 flex flex-col text-sm">

      {/* Event toast */}
      <div className={`border-b border-navy-700 overflow-hidden transition-all duration-500 ${toastVisible ? 'max-h-24 opacity-100' : 'max-h-0 opacity-0'}`}>
        {lastEvent && (
          <div className="flex items-center gap-3 px-5 py-3.5 bg-emerald-900/20 border-b border-emerald-700/30">
            <span className="w-2.5 h-2.5 rounded-full bg-emerald-400 flex-shrink-0" />
            <div>
              <div className="text-emerald-400 font-semibold text-sm">Event fired ✓</div>
              <div className="text-slate-400 truncate text-xs mt-0.5">
                {formatTitle(lastEvent.title)} ·{' '}
                {lastEvent.eventType === 'rating' ? `★${lastEvent.rating}` : lastEvent.eventType}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Your session */}
      {sessionEvents.length > 0 && (
        <div className="border-b border-navy-700 px-5 py-4">
          <div className="text-xs uppercase tracking-widest text-slate-500 mb-3 font-semibold">Your session</div>
          <div className="flex flex-col gap-2 max-h-44 overflow-y-auto">
            {[...sessionEvents].reverse().slice(0, 8).map((e, i) => (
              <div key={i} className="flex items-center justify-between bg-navy-800/50 rounded-lg px-3 py-2">
                <span className="text-slate-200 truncate flex-1 mr-2 text-sm">
                  {formatTitle(e.title)}
                </span>
                <span className={`text-xs px-2 py-0.5 rounded font-medium flex-shrink-0 ${eventBadge[e.eventType]}`}>
                  {e.eventType === 'rating' ? `★${e.rating}` : e.eventType}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Live feed */}
      <div className="flex-1 px-5 py-4 overflow-hidden border-b border-navy-700">
        <div className="flex items-center gap-2 mb-3">
          <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${feed.length > 0 ? 'bg-emerald-400 animate-ping' : 'bg-slate-600'}`} />
          <span className="text-xs uppercase tracking-widest text-slate-500 font-semibold">Live Feed</span>
        </div>
        {feed.length === 0 ? (
          <p className="text-slate-600 text-sm">No events yet. Hover a card to fire one.</p>
        ) : (
          <div className="flex flex-col gap-2 font-mono overflow-y-auto max-h-48">
            {feed.map((ev, i) => (
              <div key={i} className={`text-xs leading-snug ${i === 0 ? 'text-emerald-400' : 'text-slate-500'}`}>
                [{formatTime(ev.timestamp)}] u{ev.user_id}{' '}
                {ev.event_type === 'rating' ? `★${ev.rating}` : ev.event_type} → {ev.movie_id}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Tech stack badges */}
      <div className="px-5 py-4">
        <div className="flex flex-wrap gap-2">
          {[
            { label: 'Kafka',      color: 'text-blue-400 bg-blue-900/20 border-blue-700/30' },
            { label: 'Spark',      color: 'text-orange-400 bg-orange-900/20 border-orange-700/30' },
            { label: 'Redis',      color: 'text-emerald-400 bg-emerald-900/20 border-emerald-700/30' },
            { label: 'ALS',        color: 'text-violet-400 bg-violet-900/20 border-violet-700/30' },
            { label: 'FastAPI',    color: 'text-slate-400 bg-navy-800 border-navy-600' },
            { label: 'PostgreSQL', color: 'text-slate-400 bg-navy-800 border-navy-600' },
          ].map(({ label, color }) => (
            <span key={label} className={`text-sm px-2.5 py-1 rounded border font-medium ${color}`}>{label}</span>
          ))}
        </div>
      </div>

    </aside>
  )
}
