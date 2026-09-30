import { useCallback, useEffect, useState } from "react"

import { apiClient } from "@/lib/api-client"

/** Independent liveness and database-readiness state for the backend. */
export interface BackendStatus {
  health: boolean | null
  ready: boolean | null
  loading: boolean
  error: string | null
  updatedAt: Date | null
}

/** Poll backend health and readiness and expose a manual refresh trigger. */
export function useBackendStatus(): BackendStatus & { refresh: () => void } {
  const [status, setStatus] = useState<BackendStatus>({
    health: null,
    ready: null,
    loading: true,
    error: null,
    updatedAt: null,
  })
  const [refreshIndex, setRefreshIndex] = useState(0)

  const refresh = useCallback(() => {
    setRefreshIndex((value) => value + 1)
  }, [])

  useEffect(() => {
    const controller = new AbortController()

    async function check() {
      setStatus((current) => ({ ...current, loading: true }))
      const [health, ready] = await Promise.allSettled([
        apiClient.health(controller.signal),
        apiClient.ready(controller.signal),
      ])
      if (controller.signal.aborted) return
      const messages = [health, ready]
        .filter(
          (result): result is PromiseRejectedResult =>
            result.status === "rejected",
        )
        .map((result) =>
          result.reason instanceof Error
            ? result.reason.message
            : "Backend request failed",
        )
      setStatus({
        health: health.status === "fulfilled",
        ready: ready.status === "fulfilled",
        loading: false,
        error: messages.length ? messages.join(" · ") : null,
        updatedAt: new Date(),
      })
    }

    void check()
    const timer = window.setInterval(check, 10_000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [refreshIndex])

  return { ...status, refresh }
}
