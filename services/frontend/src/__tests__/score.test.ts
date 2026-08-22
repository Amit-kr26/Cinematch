import { describe, it, expect } from 'vitest'
import { normalizeScore } from '../utils/score'

describe('normalizeScore', () => {
  it('maps 2.5 to 50',          () => expect(normalizeScore(2.5)).toBe(50))
  it('maps 0 to 0',             () => expect(normalizeScore(0)).toBe(0))
  it('maps null to 0',          () => expect(normalizeScore(null)).toBe(0))
  it('maps undefined to 0',     () => expect(normalizeScore(undefined)).toBe(0))
  it('maps NaN to 0',           () => expect(normalizeScore(NaN)).toBe(0))
  it('caps above max at 99',    () => expect(normalizeScore(10)).toBe(99))
  it('maps 1.25 to 25',         () => expect(normalizeScore(1.25)).toBe(25))
})
