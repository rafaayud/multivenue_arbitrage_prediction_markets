import { useEffect, useState } from "react"

import { apiClient } from "@/lib/api-client"
import type { PnlDashboard, PnlView, TimeRange } from "@/types/pnl"

interface PnlState {
  pnl: PnlDashboard | null
  loading: boolean
  error: string | null
  updatedAt: Date | null
}

/** Poll the consolidated PnL endpoint without coupling it to execution events. */
export function usePnl(view: PnlView, range: TimeRange): PnlState {
  const [state, setState] = useState<PnlState>({
    pnl: null,
    loading: true,
    error: null,
    updatedAt: null,
  })

  useEffect(() => {
    const controller = new AbortController()
    setState((current) => ({ ...current, loading: true, error: null }))

    async function load() {
      try {
        const pnl = await apiClient.pnl(view, range, controller.signal)
        if (controller.signal.aborted) return
        setState({ pnl, loading: false, error: null, updatedAt: new Date() })
      } catch (reason) {
        if (controller.signal.aborted) return
        setState((current) => ({
          ...current,
          loading: false,
          error: reason instanceof Error ? reason.message : "PnL request failed",
        }))
      }
    }

    void load()
    const timer = window.setInterval(load, 30_000)
    return () => {
      controller.abort()
      window.clearInterval(timer)
    }
  }, [range, view])

  return state
}
