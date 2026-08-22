// MovieLens stores articles at end: "Wizard of Oz, The (1939)"
// This normalizes to "The Wizard of Oz" and strips the year.
const ARTICLE_RE = /^(.*),\s+(The|A|An|Les|La|Le|Los|Las|Un|Une|Der|Die|Das|Il|Lo)$/i
const YEAR_RE    = /\s*\(\d{4}\)\s*$/

export function formatTitle(raw: string | undefined | null): string {
  const stripped = (raw ?? '').replace(YEAR_RE, '').trim()
  const m = ARTICLE_RE.exec(stripped)
  return m ? `${m[2]} ${m[1]}` : stripped
}

export function extractYear(raw: string | undefined | null): string {
  return (raw ?? '').match(/\((\d{4})\)\s*$/)?.[1] ?? ''
}
