import { useState, useEffect, useRef, useCallback } from 'react'
import { PipelineStage } from '../types'

const TIMINGS = {
  EVENT_TO_KAFKA:    300,
  KAFKA_TO_SPARK:    800,
  REDIS_TO_COMPLETE: 1500,
  COMPLETE_TO_IDLE:  2000,
  SPARK_DURATION:    10,
} as const

export function usePipelineAnimation() {
  const [stage, setStage] = useState<PipelineStage>('idle')
  const [sparkCountdown, setSparkCountdown] = useState(10)
  const timers = useRef<ReturnType<typeof setTimeout>[]>([])
  const countdownInterval = useRef<ReturnType<typeof setInterval> | null>(null)

  const clearAll = () => {
    timers.current.forEach(clearTimeout)
    timers.current = []
    if (countdownInterval.current) {
      clearInterval(countdownInterval.current)
      countdownInterval.current = null
    }
  }

  const fireEvent = useCallback(() => {
    clearAll()
    setStage('event_sent')

    timers.current.push(setTimeout(() => setStage('kafka_ingesting'), TIMINGS.EVENT_TO_KAFKA))

    timers.current.push(setTimeout(() => {
      setStage('spark_processing')
      setSparkCountdown(TIMINGS.SPARK_DURATION)
      countdownInterval.current = setInterval(() => {
        setSparkCountdown(prev => {
          if (prev <= 1) {
            if (countdownInterval.current) clearInterval(countdownInterval.current)
            return 0
          }
          return prev - 1
        })
      }, 1000)
    }, TIMINGS.KAFKA_TO_SPARK))
  }, [])

  const notifyRedisUpdated = useCallback(() => {
    if (stage !== 'spark_processing' && stage !== 'kafka_ingesting' && stage !== 'event_sent') return
    clearAll()
    setStage('redis_updated')
    timers.current.push(setTimeout(() => {
      setStage('complete')
      timers.current.push(setTimeout(() => setStage('idle'), TIMINGS.COMPLETE_TO_IDLE))
    }, TIMINGS.REDIS_TO_COMPLETE))
  }, [stage])

  useEffect(() => () => clearAll(), [])

  return { stage, sparkCountdown, fireEvent, notifyRedisUpdated }
}
