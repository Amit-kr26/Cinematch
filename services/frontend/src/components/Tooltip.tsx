import { ReactNode } from 'react'

interface Props {
  label: string
  children: ReactNode
  side?: 'top' | 'bottom' | 'left'
}

export function Tooltip({ label, children, side = 'top' }: Props) {
  const pos =
    side === 'bottom'
      ? 'top-full mt-2 left-1/2 -translate-x-1/2'
      : side === 'left'
      ? 'right-full mr-2 top-1/2 -translate-y-1/2'
      : 'bottom-full mb-2 left-1/2 -translate-x-1/2'
  return (
    <span className="relative inline-flex group/tt">
      {children}
      <span
        role="tooltip"
        className={`pointer-events-none absolute ${pos} z-50 whitespace-nowrap rounded-md border border-navy-600 bg-navy-800 px-2 py-1 text-xs font-medium text-slate-100 opacity-0 shadow-xl transition-opacity duration-150 group-hover/tt:opacity-100`}
      >
        {label}
      </span>
    </span>
  )
}
