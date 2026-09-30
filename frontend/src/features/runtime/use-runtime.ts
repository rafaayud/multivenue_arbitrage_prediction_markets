import { useCallback, useEffect, useState } from "react"

import { apiClient } from "@/lib/api-client"
import type {
  RequestState,
  RuntimeState,
  SignalSettings,
} from "@/types/runtime"

/** Runtime state and commands exposed to dashboard components. */
export interface RuntimeController {
  runtime: RuntimeState | null
  requestState: RequestState
  error: string | null
  refresh: () => Promise<void>
  unmonitor: (monitorKey: string) => Promise<void>
  updateSignalSettings: (settings: SignalSettings) => Promise<void>
}

/** Poll the backend runtime and expose guarded start, stop, and refresh commands. */
export function useRuntime(): RuntimeController {
  const [runtime, setRuntime] = useState<RuntimeState | null>(null)
  const [requestState, setRequestState] = useState<RequestState>("loading")
  const [error, setError] = useState<string | null>(null)

  const execute = useCallback(
    async (operation: () => Promise<RuntimeState>) => {
      setRequestState("loading")
      setError(null)
      try {
        setRuntime(await operation())
        setRequestState("success")
      } catch (reason) {
        setRequestState("error")
        setError(
          reason instanceof Error ? reason.message : "Runtime request failed",
        )
      }
    },
    [],
  )

  const refresh = useCallback(
    () => execute(() => apiClient.runtimeStatus()),
    [execute],
  )
  const unmonitor = useCallback(
    (monitorKey: string) =>
      execute(() => apiClient.unmonitorRegularMarket(monitorKey)),
    [execute],
  )
  const updateSignalSettings = useCallback(
    (settings: SignalSettings) =>
      execute(() => apiClient.updateSignalSettings(settings)),
    [execute],
  )
  useEffect(() => {
    void refresh()
    const timer = window.setInterval(refresh, 5_000)
    return () => window.clearInterval(timer)
  }, [refresh])

  return {
    runtime,
    requestState,
    error,
    refresh,
    unmonitor,
    updateSignalSettings,
  }
}
