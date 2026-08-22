import { useState } from 'react'
import { Home, Search, ChevronDown, Film } from 'lucide-react'
import { Tooltip } from './Tooltip'

interface Props {
  search: string
  onSearch: (v: string) => void
  genre: string
  genres: string[]
  onGenre: (g: string) => void
  onHome: () => void
  userId: number
}

export function TopNav({ search, onSearch, genre, genres, onGenre, onHome, userId }: Props) {
  const [open, setOpen] = useState(false)

  return (
    <div className="bg-navy-900/80 border-b border-navy-700 px-6 py-3 flex items-center gap-3">
      <Tooltip label="Back to home" side="bottom">
        <button
          onClick={onHome}
          data-testid="home-button"
          className="flex h-10 w-10 items-center justify-center rounded-lg border border-navy-600 bg-navy-800 text-slate-300 transition-colors hover:border-accent/50 hover:text-white"
        >
          <Home size={18} />
        </button>
      </Tooltip>

      <div className="relative flex-1 max-w-md">
        <Search className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" size={17} />
        <input
          value={search}
          onChange={(e) => onSearch(e.target.value)}
          placeholder="Search movies…"
          data-testid="search-input"
          className="w-full rounded-lg border border-navy-600 bg-navy-800 py-2.5 pl-9 pr-3 text-sm text-slate-100 placeholder:text-slate-500 focus:border-accent/60 focus:outline-none focus:ring-1 focus:ring-accent/30"
        />
      </div>

      <div className="relative">
        <button
          onClick={() => setOpen((v) => !v)}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          data-testid="genre-filter-button"
          className="flex items-center gap-2 rounded-lg border border-navy-600 bg-navy-800 px-3.5 py-2.5 text-sm text-slate-200 transition-colors hover:border-accent/50"
        >
          <span className="text-slate-400">Genre:</span>
          <span className="font-medium">{genre || 'All'}</span>
          <ChevronDown size={15} className={open ? 'rotate-180 transition' : 'transition'} />
        </button>
        {open && (
          <div
            className="absolute right-0 z-50 mt-2 max-h-72 w-48 overflow-y-auto rounded-xl border border-navy-600 bg-navy-800 p-1.5 shadow-2xl"
            data-testid="genre-dropdown"
          >
            {['All', ...genres].map((g) => (
              <button
                key={g}
                onMouseDown={() => { onGenre(g); setOpen(false) }}
                data-testid={`genre-option-${g}`}
                className={`block w-full rounded-lg px-3 py-2 text-left text-sm transition-colors hover:bg-navy-700 ${
                  (genre || 'All') === g ? 'text-accent' : 'text-slate-300'
                }`}
              >
                {g}
              </button>
            ))}
          </div>
        )}
      </div>

      <Tooltip label="Your assigned profile" side="bottom">
        <span className="hidden sm:flex items-center gap-1.5 rounded-lg border border-navy-600 bg-navy-800 px-3 py-2.5 text-sm font-medium text-slate-300">
          <Film size={14} className="text-accent" /> user #{userId}
        </span>
      </Tooltip>
    </div>
  )
}
