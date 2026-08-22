import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { StatsData } from '../types'

export function useStats(pollMs = 10_000) {
  const [data, setData] = useState<StatsData | null>(null)

  const refresh = useCallback(async () => {
    try {
      const resp = await axios.get<StatsData>('/stats')
      setData(resp.data)
    } catch {
      // stats are best-effort — silently ignore
    }
  }, [])

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, pollMs)
    return () => clearInterval(id)
  }, [refresh, pollMs])

  return { stats: data, refresh }
}
