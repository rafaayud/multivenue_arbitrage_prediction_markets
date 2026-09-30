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
  opportunities: ArbitrageOpportunity[]
  signalCount: number
  lastReceivedAt: Date | null
}

const ArbitrageStreamContext =
  createContext<ArbitrageStreamContextValue | null>(null)

/** Own selected regular-event sockets plus their recent opportunities. */
export function ArbitrageStreamProvider({
  children,
}: {
  children: ReactNode
}) {
  const [connectionStatuses, setConnectionStatuses] = useState<
    Record<string, ConnectionStatus>
  >({})
  const [connectionError, setConnectionError] = useState<string | null>(null)
  const [opportunities, setOpportunities] = useState<ArbitrageOpportunity[]>([])
  const [signalCount, setSignalCount] = useState(0)
  const [lastReceivedAt, setLastReceivedAt] = useState<Date | null>(null)
  const [regularMonitorKeys, setRegularMonitorKeys] = useState<string[]>([])
  const sockets = useRef(
    new Map<string, ReturnType<typeof createArbitrageSocket>>(),
  )

  const monitorKeys = regularMonitorKeys

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
      opportunities,
      signalCount,
      lastReceivedAt,
    }),
    [
      connectionStatus,
      connectionError,
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
