import {
  MAX_RECENT_OPPORTUNITIES,
  WS_BASE_URL,
} from "@/lib/config"
import { parseArbitrageOpportunity } from "@/types/arbitrage"
import type {
  ArbitrageOpportunity,
  ConnectionStatus,
} from "@/types/arbitrage"
import {
  parseExecutionActivity,
  type ExecutionActivitySnapshot,
} from "@/types/execution"

type SocketFactory = (url: string) => WebSocket

interface ReconnectingSocketOptions<T> {
  url: string
  parse: (value: unknown) => T
  onMessage: (value: T) => void
  onStatus: (status: ConnectionStatus) => void
  onError: (message: string) => void
  socketFactory?: SocketFactory
  initialDelayMs?: number
  maxDelayMs?: number
}

/** Manage one WebSocket with bounded exponential-backoff reconnection. */
export class ReconnectingSocket<T> {
  private socket: WebSocket | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private reconnectAttempt = 0
  private stopped = true

  constructor(private readonly options: ReconnectingSocketOptions<T>) {}

  start(): void {
    if (!this.stopped || this.socket || this.reconnectTimer) {
      return
    }
    this.stopped = false
    this.connect()
  }

  stop(): void {
    this.stopped = true
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    const socket = this.socket
    this.socket = null
    if (socket && socket.readyState < WebSocket.CLOSING) {
      socket.close(1000, "Client stopped")
    }
    this.options.onStatus("disconnected")
  }

  private connect(): void {
    if (this.stopped || this.socket) {
      return
    }
    this.options.onStatus("connecting")
    const factory =
      this.options.socketFactory ?? ((url: string) => new WebSocket(url))
    try {
      const socket = factory(this.options.url)
      this.socket = socket
      socket.onopen = () => {
        if (socket !== this.socket) return
        this.reconnectAttempt = 0
        this.options.onStatus("connected")
      }
      socket.onmessage = (event) => {
        try {
          this.options.onMessage(
            this.options.parse(JSON.parse(String(event.data))),
          )
        } catch (error) {
          this.options.onError(
            error instanceof Error ? error.message : "Invalid WebSocket message",
          )
        }
      }
      socket.onerror = () => {
        if (socket !== this.socket) return
        this.options.onStatus("error")
        this.options.onError("WebSocket transport error")
        if (socket.readyState < WebSocket.CLOSING) {
          socket.close()
        }
      }
      socket.onclose = () => {
        if (socket !== this.socket) return
        this.socket = null
        if (this.stopped) {
          this.options.onStatus("disconnected")
          return
        }
        this.scheduleReconnect()
      }
    } catch (error) {
      this.socket = null
      this.options.onStatus("error")
      this.options.onError(
        error instanceof Error ? error.message : "WebSocket connection failed",
      )
      this.scheduleReconnect()
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer) {
      return
    }
    this.options.onStatus("connecting")
    const initial = this.options.initialDelayMs ?? 1_000
    const maximum = this.options.maxDelayMs ?? 15_000
    const delay = Math.min(initial * 2 ** this.reconnectAttempt, maximum)
    this.reconnectAttempt += 1
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, delay)
  }
}

/** Create the authenticated stream of persisted orders, trades and positions. */
export function createExecutionActivitySocket(handlers: {
  onMessage: (value: ExecutionActivitySnapshot) => void
  onStatus: (status: ConnectionStatus) => void
  onError: (message: string) => void
}): ReconnectingSocket<ExecutionActivitySnapshot> {
  return new ReconnectingSocket({
    url: `${WS_BASE_URL}/ws/execution-events`,
    parse: parseExecutionActivity,
    ...handlers,
  })
}

/** Build the arbitrage signal URL for one monitored market. */
export function arbitrageSocketUrl(monitorKey: string): string {
  const query = new URLSearchParams({ monitor_key: monitorKey })
  return `${WS_BASE_URL}/ws/arbitrage-signals?${query.toString()}`
}

/** Create a reconnecting socket that validates incoming arbitrage messages. */
export function createArbitrageSocket(
  monitorKey: string,
  handlers: {
    onMessage: (value: ArbitrageOpportunity) => void
    onStatus: (status: ConnectionStatus) => void
    onError: (message: string) => void
  },
): ReconnectingSocket<ArbitrageOpportunity> {
  return new ReconnectingSocket({
    url: arbitrageSocketUrl(monitorKey),
    parse: parseArbitrageOpportunity,
    ...handlers,
  })
}

/** Prepend a non-duplicate opportunity while enforcing the history limit. */
export function prependOpportunity(
  current: ArbitrageOpportunity[],
  next: ArbitrageOpportunity,
): ArbitrageOpportunity[] {
  return [
    next,
    ...current.filter((opportunity) => opportunity.id !== next.id),
  ].slice(0, MAX_RECENT_OPPORTUNITIES)
}
