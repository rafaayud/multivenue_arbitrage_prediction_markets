import { useEffect, useState } from "react"

import { apiClient } from "@/lib/api-client"
import type { VenueHealthReport } from "@/types/venue-health"

interface VenueHealthState {
  report: VenueHealthReport | null
  loading: boolean
  error: string | null
}

/** Poll the cached venue-health report independently of execution streams. */
export function useVenueHealth(): VenueHealthState {
  const [state, setState] = useState<VenueHealthState>({
    report: null,
    loading: true,
    error: null,
  })

  useEffect(() => {
    const controller = new AbortController()

    async function load() {
      try {
        const report = await apiClient.venueHealth(controller.signal)
        if (!controller.signal.aborted) {
          setState({ report, loading: false, error: null })
        }
      } catch (reason) {
        if (!controller.signal.aborted) {
          setState((current) => ({
            ...current,
            loading: false,
            error:
              reason instanceof Error
                ? reason.message
                : "Venue health request failed",
          }))
        }
      }
    }

    void load()
    const timer = window.setInterval(load, 60_000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [])

  return state
}
