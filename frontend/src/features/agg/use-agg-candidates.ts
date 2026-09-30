import { useEffect, useState } from "react"

import { apiClient } from "@/lib/api-client"
import type { ArbitrageCandidate } from "@/types/agg"

interface AggCandidatesState {
  candidates: ArbitrageCandidate[]
  loading: boolean
  error: string | null
  updatedAt: Date | null
}

export interface ArbitrageCandidateFilters {
  topic?: string
  searchText?: string
  liveOnly?: boolean
}

export type AggDiscoveryView = "opportunities" | "markets"

/** Poll the selected read-only AGG discovery view and retain its latest snapshot. */
export function useArbitrageCandidates(
  filters: ArbitrageCandidateFilters = {},
  view: AggDiscoveryView = "opportunities",
): AggCandidatesState {
  const { topic, searchText, liveOnly = false } = filters
  const [state, setState] = useState<AggCandidatesState>({
    candidates: [],
    loading: true,
    error: null,
    updatedAt: null,
  })

  useEffect(() => {
    const controller = new AbortController()
    setState({
      candidates: [],
      loading: true,
      error: null,
      updatedAt: null,
    })

    async function load() {
      try {
        const options = {
          limit: 100,
          topics: topic ? [topic] : undefined,
          searchText,
          liveOnly,
          signal: controller.signal,
        }
        const candidates =
          view === "markets"
            ? await apiClient.marketCatalog(options)
            : await apiClient.arbitrageCandidates(options)
        if (controller.signal.aborted) return
        setState({
          candidates,
          loading: false,
          error: null,
          updatedAt: new Date(),
        })
      } catch (reason) {
        if (controller.signal.aborted) return
        setState((current) => ({
          ...current,
          loading: false,
          error: reason instanceof Error ? reason.message : "AGG candidates failed",
          updatedAt: new Date(),
        }))
      }
    }

    void load()
    const timer = window.setInterval(load, 60_000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [liveOnly, searchText, topic, view])

  return state
}
