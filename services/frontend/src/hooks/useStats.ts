import { useState, useEffect } from 'react'
import axios from 'axios'
import { StatsData } from '../types'

export function useStats(pollMs = 10_000) {
  const [data, setData] = useState<StatsData | null>(null)

  useEffect(() => {
    let cancelled = false

    const fetch = async () => {
      try {
        const resp = await axios.get<StatsData>('/stats')
        if (!cancelled) setData(resp.data)
      } catch {
        // stats are best-effort — silently ignore
      }
    }

    fetch()
    const id = setInterval(fetch, pollMs)
    return () => { cancelled = true; clearInterval(id) }
  }, [pollMs])

  return data
}
