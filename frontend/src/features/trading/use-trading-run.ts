import { useCallback, useEffect, useState } from "react"

import { apiClient } from "@/lib/api-client"
import type { RequestState } from "@/types/runtime"
import type { TradingRun, TradingRunSettings } from "@/types/trading"

export interface TradingRunController {
  run: TradingRun | null
  requestState: RequestState
  error: string | null
  enable: (
    tradingKey: string,
    settings: TradingRunSettings,
  ) => Promise<void>
  disable: () => Promise<void>
}

export function useTradingRun(): TradingRunController {
  const [run, setRun] = useState<TradingRun | null>(null)
  const [requestState, setRequestState] = useState<RequestState>("loading")
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      setRun(await apiClient.currentTradingRun())
      setRequestState("success")
      setError(null)
    } catch (reason) {
      setRequestState("error")
      setError(
        reason instanceof Error ? reason.message : "Trading status failed",
      )
    }
  }, [])

  const enable = useCallback(async (
    tradingKey: string,
    settings: TradingRunSettings,
  ) => {
    setRequestState("loading")
    setError(null)
    try {
      await apiClient.createTradingSession(tradingKey)
      setRun(await apiClient.startTrading(settings))
      setRequestState("success")
    } catch (reason) {
      setRequestState("error")
      setError(reason instanceof Error ? reason.message : "Trading start failed")
    }
  }, [])

  const disable = useCallback(async () => {
    if (!run) return
    setRequestState("loading")
    setError(null)
    try {
      setRun(await apiClient.stopTrading(run.id))
      setRequestState("success")
    } catch (reason) {
      setRequestState("error")
      setError(reason instanceof Error ? reason.message : "Trading stop failed")
    }
  }, [run])

  useEffect(() => {
    void refresh()
    const timer = window.setInterval(refresh, 5_000)
    return () => window.clearInterval(timer)
  }, [refresh])

  return { run, requestState, error, enable, disable }
}
