import {
  useCallback,
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react"

import { ApiError, apiClient } from "@/lib/api-client"
import { createExecutionActivitySocket } from "@/lib/websocket-client"
import type { ReconnectingSocket } from "@/lib/websocket-client"
import type { ConnectionStatus } from "@/types/arbitrage"
import type { ExecutionActivitySnapshot } from "@/types/execution"

interface ExecutionActivityContextValue {
  snapshot: ExecutionActivitySnapshot | null
  connectionStatus: ConnectionStatus
  connectionError: string | null
  lastReceivedAt: Date | null
  authenticate: (tradingKey: string) => Promise<void>
}

const ExecutionActivityContext =
  createContext<ExecutionActivityContextValue | null>(null)

/** Own one authenticated execution stream shared by every reporting page. */
export function ExecutionActivityProvider({
  children,
}: {
  children: ReactNode
}) {
  const [snapshot, setSnapshot] =
    useState<ExecutionActivitySnapshot | null>(null)
  const [connectionStatus, setConnectionStatus] =
    useState<ConnectionStatus>("disconnected")
  const [connectionError, setConnectionError] = useState<string | null>(
    null,
  )
  const [lastReceivedAt, setLastReceivedAt] = useState<Date | null>(null)
  const socketRef =
    useRef<ReconnectingSocket<ExecutionActivitySnapshot> | null>(null)

  useEffect(() => {
    let active = true
    const socket = createExecutionActivitySocket({
      onMessage: (message) => {
        setSnapshot(message)
        setConnectionError(null)
        setLastReceivedAt(new Date())
      },
      onStatus: (status) => {
        setConnectionStatus(status)
        if (status === "connected") setConnectionError(null)
      },
      onError: setConnectionError,
    })
    socketRef.current = socket
    void apiClient
      .tradingSession()
      .then(() => {
        if (active) socket.start()
      })
      .catch((error: unknown) => {
        if (!active) return
        setConnectionError(
          error instanceof ApiError && error.status === 401
            ? "Authentication required"
            : error instanceof Error
              ? error.message
              : "Authentication check failed",
        )
      })
    return () => {
      active = false
      socketRef.current = null
      socket.stop()
    }
  }, [])

  const authenticate = useCallback(async (tradingKey: string) => {
    try {
      await apiClient.createTradingSession(tradingKey)
      setConnectionError(null)
      socketRef.current?.stop()
      socketRef.current?.start()
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Authentication failed"
      setConnectionError(message)
      throw error
    }
  }, [])

  const value = useMemo(
    () => ({
      snapshot,
      connectionStatus,
      connectionError,
      lastReceivedAt,
      authenticate,
    }),
    [
      snapshot,
      connectionStatus,
      connectionError,
      lastReceivedAt,
      authenticate,
    ],
  )

  return (
    <ExecutionActivityContext.Provider value={value}>
      {children}
    </ExecutionActivityContext.Provider>
  )
}

/** Read the shared persisted execution activity stream. */
export function useExecutionActivity(): ExecutionActivityContextValue {
  const context = useContext(ExecutionActivityContext)
  if (!context) {
    throw new Error(
      "useExecutionActivity must be used inside ExecutionActivityProvider",
    )
  }
  return context
}
