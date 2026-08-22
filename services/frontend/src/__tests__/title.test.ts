import { describe, it, expect } from 'vitest'
import { formatTitle, extractYear } from '../utils/title'

describe('formatTitle', () => {
  it('reinverts article "Wizard of Oz, The"', () =>
    expect(formatTitle('Wizard of Oz, The (1939)')).toBe('The Wizard of Oz'))
  it('reinverts "Matrix, The"', () =>
    expect(formatTitle('Matrix, The (1999)')).toBe('The Matrix'))
  it('leaves normal title unchanged', () =>
    expect(formatTitle('Inception (2010)')).toBe('Inception'))
  it('strips year from result', () =>
    expect(formatTitle('Toy Story (1995)')).toBe('Toy Story'))
  it('handles null',      () => expect(formatTitle(null)).toBe(''))
  it('handles undefined', () => expect(formatTitle(undefined)).toBe(''))
  it('handles "A" article', () =>
    expect(formatTitle('Bug\'s Life, A (1998)')).toBe('A Bug\'s Life'))
})

describe('extractYear', () => {
  it('extracts 4-digit year',       () => expect(extractYear('Pulp Fiction (1994)')).toBe('1994'))
  it('returns empty if no year',    () => expect(extractYear('No Year')).toBe(''))
  it('handles null',                () => expect(extractYear(null)).toBe(''))
  it('handles undefined',           () => expect(extractYear(undefined)).toBe(''))
})
