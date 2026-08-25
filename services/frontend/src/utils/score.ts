const ALS_SCORE_MAX = 5.0

export function normalizeScore(raw: number | undefined | null): number {
  if (!raw || !Number.isFinite(raw)) return 0
  return Math.min(Math.round((raw / ALS_SCORE_MAX) * 99), 99)
}
