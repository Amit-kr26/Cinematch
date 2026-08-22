import { Sparkles, Star, Heart, Eye, ArrowRight } from 'lucide-react'

interface Props {
  userId: number
  onClose: () => void
}

export function WelcomeModal({ userId, onClose }: Props) {
  return (
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 backdrop-blur-sm px-4"
      data-testid="welcome-modal"
    >
      <div className="relative w-full max-w-xl overflow-hidden rounded-2xl border border-navy-600 bg-navy-900 shadow-2xl animate-scale-in">
        <div
          className="absolute inset-0 opacity-25"
          style={{
            backgroundImage:
              'url(https://images.pexels.com/photos/18501410/pexels-photo-18501410.jpeg?auto=compress&cs=tinysrgb&dpr=2&h=650&w=940)',
            backgroundSize: 'cover',
            backgroundPosition: 'center',
          }}
        />
        <div className="absolute inset-0 bg-gradient-to-t from-navy-900 via-navy-900/90 to-navy-900/40" />

        <div className="relative p-8 sm:p-10">
          <div className="mb-4 inline-flex items-center gap-2 rounded-full border border-accent/40 bg-accent/10 px-3 py-1 text-xs uppercase tracking-[0.2em] text-accent">
            <Sparkles size={13} /> CineMatch
          </div>
          <h1 className="font-display text-3xl sm:text-4xl font-bold text-white">
            Welcome to your movie night
          </h1>
          <p className="mt-3 text-slate-300 leading-relaxed">
            You&apos;ve been dropped into a real MovieLens viewer profile
            <span className="mx-1 rounded bg-navy-700 px-1.5 py-0.5 font-mono text-slate-200">
              user #{userId}
            </span>
            . Browse the catalogue and <strong>like</strong>, <strong>rate</strong> or{' '}
            <strong>view</strong> movies you enjoy — events stream through Kafka → Spark and your
            top picks re-rank live.
          </p>

          <div className="mt-6 grid grid-cols-3 gap-3 text-center">
            {[
              { icon: <Heart size={18} />, label: 'Like it' },
              { icon: <Star size={18} />, label: 'Rate 1–5★' },
              { icon: <Eye size={18} />, label: 'Mark viewed' },
            ].map((s) => (
              <div key={s.label} className="rounded-xl border border-navy-600 bg-black/30 px-3 py-4">
                <div className="mx-auto mb-2 flex h-9 w-9 items-center justify-center rounded-full bg-accent/15 text-accent">
                  {s.icon}
                </div>
                <div className="text-sm text-slate-300">{s.label}</div>
              </div>
            ))}
          </div>

          <button
            onClick={onClose}
            data-testid="welcome-get-started-button"
            className="mt-7 inline-flex w-full items-center justify-center gap-2 rounded-xl bg-accent px-6 py-3.5 text-base font-bold text-white transition-colors hover:bg-accent-light"
          >
            Get started <ArrowRight size={18} />
          </button>
        </div>
      </div>
    </div>
  )
}
