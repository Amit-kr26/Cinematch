import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { RecommendationList } from '../components/RecommendationList'
import { RecsResponse } from '../types'

const makeData = (source: RecsResponse['source'] = 'redis'): RecsResponse => ({
  user_id:    1,
  updated_at: new Date().toISOString(),
  source,
  variant:    'control',
  recs: [
    { movie_id: 1, title: 'Movie Alpha (2001)', genres: 'Drama', score: 1.1 },
    { movie_id: 2, title: 'Movie Beta (2002)',  genres: 'Action', score: 1.2 },
  ],
})

describe('RecommendationList', () => {
  it('shows error banner when error provided', () => {
    render(<RecommendationList data={null} loading={false} error="API down"
      onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText(/API down/)).toBeInTheDocument()
  })

  it('shows skeleton grid while loading with no data', () => {
    const { container } = render(
      <RecommendationList data={null} loading={true} error={null}
        onFireEvent={vi.fn()} onCardClick={vi.fn()} />
    )
    expect(container.querySelectorAll('.skeleton').length).toBeGreaterThan(0)
  })

  it('renders movie titles when recs provided', () => {
    render(<RecommendationList data={makeData()} loading={false} error={null}
      onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText('Movie Alpha')).toBeInTheDocument()
    expect(screen.getByText('Movie Beta')).toBeInTheDocument()
  })

  it('shows control-group banner regardless of source', () => {
    render(<RecommendationList data={makeData('als_baseline')} loading={false} error={null}
      onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText(/Control group/)).toBeInTheDocument()
    expect(screen.getByText(/do not reorder this list/)).toBeInTheDocument()
  })

  it('shows treatment-personalized banner for re-ranked sources', () => {
    const data: RecsResponse = { ...makeData('redis'), variant: 'treatment' }
    render(<RecommendationList data={data} loading={false} error={null}
      onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText(/re-ranked from your events/)).toBeInTheDocument()
  })

  it('shows treatment-baseline banner when nothing re-ranked yet', () => {
    const data: RecsResponse = { ...makeData('als_baseline'), variant: 'treatment' }
    render(<RecommendationList data={data} loading={false} error={null}
      onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText(/nothing re-ranked\s+yet/i)).toBeInTheDocument()
  })

  it('shows empty state for source=empty', () => {
    const emptyData: RecsResponse = { ...makeData('empty'), recs: [] }
    render(<RecommendationList data={emptyData} loading={false} error={null}
      onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText(/No recommendations yet/)).toBeInTheDocument()
  })
})
