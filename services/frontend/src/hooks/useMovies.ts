import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import { Movie, MoviesResponse } from '../types'

export function useMovies(search: string, genre: string, pageSize = 20) {
  const [movies, setMovies]           = useState<Movie[]>([])
  const [page, setPage]               = useState(1)
  const [hasMore, setHasMore]         = useState(false)
  const [total, setTotal]             = useState(0)
  const [loading, setLoading]         = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError]             = useState<string | null>(null)

  const fetchPage = useCallback(async (p: number, replace: boolean) => {
    if (replace) setLoading(true)
    else setLoadingMore(true)
    if (replace) setError(null)
    try {
      const r = await axios.get<MoviesResponse>('/movies', {
        params: {
          search: search || undefined,
          genre: genre !== 'All' ? genre : undefined,
          page: p,
          page_size: pageSize,
        },
      })
      setMovies((prev) => (replace ? r.data.movies : [...prev, ...r.data.movies]))
      setHasMore(r.data.has_more)
      setTotal(r.data.total)
      setPage(p)
    } catch (e) {
      if (replace) setError(e instanceof Error ? e.message : 'Failed to load movies')
    } finally {
      setLoading(false)
      setLoadingMore(false)
    }
  }, [search, genre, pageSize])

  useEffect(() => { fetchPage(1, true) }, [fetchPage])

  const loadMore = useCallback(() => {
    if (hasMore && !loadingMore) fetchPage(page + 1, false)
  }, [hasMore, loadingMore, page, fetchPage])

  const retry = useCallback(() => fetchPage(1, true), [fetchPage])

  return { movies, total, hasMore, loading, loadingMore, loadMore, error, retry }
}
