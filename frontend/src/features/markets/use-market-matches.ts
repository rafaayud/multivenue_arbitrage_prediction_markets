import { useEffect, useState } from "react"

import { apiClient } from "@/lib/api-client"
import type { MarketMatchesResponse } from "@/types/api"

interface MarketMatchesState {
  matches: MarketMatchesResponse[]
  loading: boolean
  error: string | null
  updatedAt: Date | null
}

/** Poll the backend-owned catalog of monitored recurring markets. */
export function useMarketMatches(): MarketMatchesState {
  const [state, setState] = useState<MarketMatchesState>({
    matches: [],
    loading: true,
    error: null,
    updatedAt: null,
  })

  useEffect(() => {
    const controller = new AbortController()

    async function load() {
      try {
        const matches = await apiClient.marketMatchesAll(controller.signal)
        if (controller.signal.aborted) return
        setState({
          matches,
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
            reason instanceof Error
              ? reason.message
              : "Market catalog request failed",
        }))
      }
    }

    void load()
    const timer = window.setInterval(load, 15_000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [])

  return state
}
