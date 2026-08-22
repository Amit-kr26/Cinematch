import { useState, useEffect, useCallback, useRef } from 'react'
import axios from 'axios'
import { RecsResponse } from '../types'

export function useRecommendations(userId: number | null, pollMs = 5000) {
  const [data, setData]          = useState<RecsResponse | null>(null)
  const [loading, setLoading]    = useState(false)
  const [error, setError]        = useState<string | null>(null)
  const [wsConnected, setWsConn] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)
  const lastPushAt = useRef(0)

  // WebSocket path — receives push updates when Spark writes new recs
  useEffect(() => {
    if (!userId) return
    const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${proto}://${window.location.host}/ws/recommend/${userId}`)
    wsRef.current = ws
    ws.onopen    = () => setWsConn(true)
    ws.onclose   = () => setWsConn(false)
    ws.onerror   = () => setWsConn(false)
    ws.onmessage = (e) => {
      try {
        lastPushAt.current = Date.now()
        const parsed = JSON.parse(e.data)
        // Streaming job may push a bare recs array; wrap it into the response envelope.
        const next: RecsResponse = Array.isArray(parsed)
          ? { user_id: userId, recs: parsed, source: 'redis',
              updated_at: new Date().toISOString(), variant: 'control' }
          : parsed
        setData(next)
        setError(null)
      } catch { /* ignore parse errors */ }
    }
    return () => { ws.close(); setWsConn(false) }
  }, [userId])

  // HTTP polling fallback — only active when WebSocket is not connected
  const fetchRecs = useCallback(async () => {
    if (!userId) return
    if (Date.now() - lastPushAt.current < 2500) return   // WS already delivered fresh recs
    setLoading(true)
    try {
      const resp = await axios.get<RecsResponse>(`/recommend/${userId}`)
      setData(resp.data)
      setError(null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }, [userId])

  useEffect(() => {
    if (wsConnected) return
    fetchRecs()
    if (!userId) return
    const id = setInterval(fetchRecs, pollMs)
    return () => clearInterval(id)
  }, [wsConnected, fetchRecs, userId, pollMs])

  return { data, loading, error, wsConnected, refetch: fetchRecs }
}
