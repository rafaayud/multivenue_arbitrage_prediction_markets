import { useEffect, useRef, useState } from "react"

import { apiClient } from "@/lib/api-client"
import { pipelineMetrics } from "@/lib/prometheus"
import type { PipelineMetrics } from "@/types/metrics"

interface LatencyMetricsState {
  pipeline: PipelineMetrics
  loading: boolean
  error: string | null
  updatedAt: Date | null
}

/** Poll Prometheus metrics and expose pipeline diagnostics. */
export function useLatencyMetrics(): LatencyMetricsState {
  const previous = useRef<{ source: string; at: number } | null>(null)
  const [state, setState] = useState<LatencyMetricsState>({
    pipeline: pipelineMetrics(""),
    loading: true,
    error: null,
    updatedAt: null,
  })

  useEffect(() => {
    const controller = new AbortController()

    async function load() {
      try {
        const source = await apiClient.metrics(controller.signal)
        if (controller.signal.aborted) return
        const now = performance.now()
        const before = previous.current
        previous.current = { source, at: now }
        setState({
          pipeline: pipelineMetrics(
            source,
            before?.source,
            before ? (now - before.at) / 1_000 : undefined,
          ),
          loading: false,
          error: null,
          updatedAt: new Date(),
        })
      } catch (reason) {
        if (controller.signal.aborted) return
        setState((current) => ({
          ...current,
          loading: false,
          error:
            reason instanceof Error ? reason.message : "Metrics request failed",
        }))
      }
    }

    void load()
    const timer = window.setInterval(load, 10_000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [])

  return state
}
