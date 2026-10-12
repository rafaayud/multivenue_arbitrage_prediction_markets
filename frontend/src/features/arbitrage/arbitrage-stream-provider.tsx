import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react"

import { apiClient } from "@/lib/api-client"
import { MAX_RECENT_OPPORTUNITIES } from "@/lib/config"
import {
  createArbitrageSocket,
  prependOpportunity,
} from "@/lib/websocket-client"
import type {
  ArbitrageOpportunity,
  ConnectionStatus,
} from "@/types/arbitrage"

interface ArbitrageStreamContextValue {
  connectionStatus: ConnectionStatus
  connectionError: string | null
  historyError: string | null
  opportunities: ArbitrageOpportunity[]
  signalCount: number
  lastReceivedAt: Date | null
}

const ArbitrageStreamContext =
  createContext<ArbitrageStreamContextValue | null>(null)

/** Combine persisted event history with selected events' live signal sockets. */
export function ArbitrageStreamProvider({
  children,
}: {
  children: ReactNode
}) {
  const [connectionStatuses, setConnectionStatuses] = useState<
    Record<string, ConnectionStatus>
  >({})
  const [connectionError, setConnectionError] = useState<string | null>(null)
  const [historyError, setHistoryError] = useState<string | null>(null)
  const [opportunities, setOpportunities] = useState<ArbitrageOpportunity[]>([])
  const [signalCount, setSignalCount] = useState(0)
  const [lastReceivedAt, setLastReceivedAt] = useState<Date | null>(null)
  const [regularMonitorKeys, setRegularMonitorKeys] = useState<string[]>([])
  const sockets = useRef(
    new Map<string, ReturnType<typeof createArbitrageSocket>>(),
  )

  const monitorKeys = regularMonitorKeys

  useEffect(() => {
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout>
    const refreshHistory = async () => {
      try {
        const history = await apiClient.arbitrageOpportunities(controller.signal)
        if (controller.signal.aborted) return
        setOpportunities((current) =>
          [...new Map([...history, ...current]
            .filter((item) => item.monitorType === "regular")
            .map((item) => [item.id, item])).values()]
            .sort((a, b) => Date.parse(b.generatedAt) - Date.parse(a.generatedAt))
            .slice(0, MAX_RECENT_OPPORTUNITIES),
        )
        setHistoryError(null)
      } catch {
        if (!controller.signal.aborted) {
          setHistoryError("Saved opportunity history could not be loaded. Retrying automatically.")
        }
      } finally {
        if (!controller.signal.aborted) {
          timer = setTimeout(refreshHistory, 10_000)
        }
      }
    }
    void refreshHistory()
    return () => {
      controller.abort()
      clearTimeout(timer)
    }
  }, [])

  useEffect(() => {
    const refreshMonitoredMarkets = async () => {
      try {
        const runtime = await apiClient.runtimeStatus()
        const nextRegular = (runtime.regular_markets ?? [])
          .map((market) => market.monitor_key)
          .sort()
        setRegularMonitorKeys((current) =>
          current.join("\0") === nextRegular.join("\0")
            ? current
            : nextRegular,
        )
      } catch {
        // Runtime health is reported elsewhere; keep existing sockets alive.
      }
    }
    void refreshMonitoredMarkets()
    const timer = window.setInterval(refreshMonitoredMarkets, 5_000)
    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    const desired = new Set(monitorKeys)
    for (const [key, socket] of sockets.current) {
      if (!desired.has(key)) {
        socket.stop()
        sockets.current.delete(key)
        setConnectionStatuses((current) => {
          const next = { ...current }
          delete next[key]
          return next
        })
      }
    }
    for (const key of monitorKeys) {
      if (sockets.current.has(key)) continue
      const socket = createArbitrageSocket(key, {
        onMessage: (opportunity) => {
          setConnectionError(null)
          setOpportunities((current) =>
            prependOpportunity(current, opportunity),
          )
          setSignalCount((current) => current + 1)
          setLastReceivedAt(new Date())
        },
        onStatus: (status) => {
          setConnectionStatuses((current) => ({
            ...current,
            [key]: status,
          }))
        },
        onError: (message) => setConnectionError(`${key}: ${message}`),
      })
      sockets.current.set(key, socket)
      socket.start()
    }
  }, [monitorKeys])

  useEffect(
    () => () => {
      sockets.current.forEach((socket) => socket.stop())
      sockets.current.clear()
    },
    [],
  )

  const statuses = Object.values(connectionStatuses)
  const connectionStatus: ConnectionStatus =
    monitorKeys.length > 0 &&
    statuses.length === monitorKeys.length &&
    statuses.every((status) => status === "connected")
      ? "connected"
      : statuses.some((status) => status === "error")
        ? "error"
        : statuses.some((status) => status === "connecting")
          ? "connecting"
          : "disconnected"

  const value = useMemo(
    () => ({
      connectionStatus,
      connectionError,
      historyError,
      opportunities,
      signalCount,
      lastReceivedAt,
    }),
    [
      connectionStatus,
      connectionError,
      historyError,
      opportunities,
      signalCount,
      lastReceivedAt,
    ],
  )

  return (
    <ArbitrageStreamContext.Provider value={value}>
      {children}
    </ArbitrageStreamContext.Provider>
  )
}

/**
 * Read the shared arbitrage stream state.
 *
 * @throws When called outside `ArbitrageStreamProvider`.
 */
export function useArbitrageStream(): ArbitrageStreamContextValue {
  const context = useContext(ArbitrageStreamContext)
  if (!context) {
    throw new Error(
      "useArbitrageStream must be used inside ArbitrageStreamProvider",
    )
  }
  return context
}
