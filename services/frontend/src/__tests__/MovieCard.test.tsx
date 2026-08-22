import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MovieCard } from '../components/MovieCard'

const mockRec = { movie_id: 1, title: 'Inception (2010)', genres: 'Action|Sci-Fi', score: 1.5 }

describe('MovieCard', () => {
  it('renders title without year', () => {
    render(<MovieCard rec={mockRec} index={0} onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText('Inception')).toBeInTheDocument()
  })

  it('shows year separately', () => {
    render(<MovieCard rec={mockRec} index={0} onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText('2010')).toBeInTheDocument()
  })

  it('shows first genre chip', () => {
    render(<MovieCard rec={mockRec} index={0} onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText('Action')).toBeInTheDocument()
  })

  it('shows normalized score badge (1.5 → 30)', () => {
    render(<MovieCard rec={mockRec} index={0} onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText('30')).toBeInTheDocument()
  })

  it('score of 0 shows 0 badge', () => {
    const zeroRec = { ...mockRec, score: 0 }
    render(<MovieCard rec={zeroRec} index={0} onFireEvent={vi.fn()} onCardClick={vi.fn()} />)
    expect(screen.getByText('0')).toBeInTheDocument()
  })
})
