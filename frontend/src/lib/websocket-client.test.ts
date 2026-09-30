import { afterEach, describe, expect, test, vi } from "vitest"

import {
  prependOpportunity,
  ReconnectingSocket,
} from "@/lib/websocket-client"
import type { ArbitrageOpportunity } from "@/types/arbitrage"

class FakeSocket {
  readyState = 0
  onopen: ((event: Event) => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null
  onerror: ((event: Event) => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null

  open() {
    this.readyState = 1
    this.onopen?.(new Event("open"))
  }

  disconnect() {
    this.readyState = 3
    this.onclose?.(new CloseEvent("close"))
  }

  fail() {
    this.onerror?.(new Event("error"))
  }

  close() {
    this.disconnect()
  }
}

afterEach(() => {
  vi.useRealTimers()
})

function opportunity(
  id: string,
  generatedAt: string,
  limitPrice: number,
): ArbitrageOpportunity {
  return {
    id,
    monitorType: "cycle",
    monitorKey: "cycle:BTC:300",
    marketLabel: "BTC",
    underlying: "BTC",
    intervalSeconds: 300,
    side: "LONG",
    generatedAt,
    signals: [
      {
        contractId: "poly:yes",
        venueId: "polymarket",
        direction: "buy",
        quantity: 2,
        limitPrice,
        fairProbability: 0.55,
        edge: 0.09,
        strategyId: "long-arbitrage",
        generatedAt,
      },
      {
        contractId: "limitless:no",
        venueId: "limitless",
        direction: "buy",
        quantity: 2,
        limitPrice: 0.5,
        fairProbability: 0.55,
        edge: 0.09,
        strategyId: "long-arbitrage",
        generatedAt,
      },
    ],
  }
}

describe("ReconnectingSocket", () => {
  test("reconnects with backoff and does not duplicate active connections", () => {
    vi.useFakeTimers()
    const sockets: FakeSocket[] = []
    const statuses: string[] = []
    const client = new ReconnectingSocket({
      url: "ws://localhost/ws",
      parse: (value) => value,
      onMessage: vi.fn(),
      onStatus: (status) => statuses.push(status),
      onError: vi.fn(),
      socketFactory: () => {
        const socket = new FakeSocket()
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
      initialDelayMs: 1_000,
    })

    client.start()
    client.start()
    expect(sockets).toHaveLength(1)
    sockets[0]!.open()
    expect(statuses.at(-1)).toBe("connected")

    sockets[0]!.disconnect()
    expect(statuses.at(-1)).toBe("connecting")
    vi.advanceTimersByTime(999)
    expect(sockets).toHaveLength(1)
    vi.advanceTimersByTime(1)
    expect(sockets).toHaveLength(2)

    client.stop()
    expect(statuses.at(-1)).toBe("disconnected")
  })

  test("reconnects after a transport error", () => {
    vi.useFakeTimers()
    const sockets: FakeSocket[] = []
    const statuses: string[] = []
    const onError = vi.fn()
    const client = new ReconnectingSocket({
      url: "ws://localhost/ws",
      parse: (value) => value,
      onMessage: vi.fn(),
      onStatus: (status) => statuses.push(status),
      onError,
      socketFactory: () => {
        const socket = new FakeSocket()
        sockets.push(socket)
        return socket as unknown as WebSocket
      },
      initialDelayMs: 1_000,
    })

    client.start()
    sockets[0]!.open()
    sockets[0]!.fail()

    expect(onError).toHaveBeenCalledWith("WebSocket transport error")
    expect(statuses).toContain("error")
    expect(statuses.at(-1)).toBe("connecting")
    vi.advanceTimersByTime(1_000)
    expect(sockets).toHaveLength(2)
  })
})

describe("prependOpportunity", () => {
  test("replaces an existing opportunity instead of duplicating it", () => {
    const previous = opportunity("stable", "2026-07-23T14:30:00Z", 0.4)
    const other = opportunity("other", "2026-07-23T14:30:30Z", 0.3)
    const latest = opportunity("stable", "2026-07-23T14:31:00Z", 0.42)

    expect(prependOpportunity([other, previous], latest)).toEqual([
      latest,
      other,
    ])
  })
})
