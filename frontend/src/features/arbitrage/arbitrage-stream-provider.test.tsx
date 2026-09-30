import { act, render, screen, waitFor } from "@testing-library/react"
import { beforeEach, describe, expect, test, vi } from "vitest"

import {
  ArbitrageStreamProvider,
  useArbitrageStream,
} from "@/features/arbitrage/arbitrage-stream-provider"
import type {
  ArbitrageOpportunity,
  ConnectionStatus,
} from "@/types/arbitrage"

interface FakeSocket {
  monitorKey: string
  handlers: {
    onMessage: (opportunity: ArbitrageOpportunity) => void
    onStatus: (status: ConnectionStatus) => void
  }
  start: ReturnType<typeof vi.fn>
  stop: ReturnType<typeof vi.fn>
}

const socketState = vi.hoisted(() => ({
  sockets: [] as FakeSocket[],
  regularMonitorKeys: [] as string[],
}))

vi.mock("@/lib/api-client", () => ({
  apiClient: {
    runtimeStatus: vi.fn(async () => ({
      running: true,
      regular_markets: socketState.regularMonitorKeys.map((monitor_key) => ({
        monitor_key,
        pair_count: 1,
        markets: [],
        pairs: [],
      })),
    })),
  },
}))

vi.mock("@/lib/websocket-client", async (importOriginal) => {
  const original =
    await importOriginal<typeof import("@/lib/websocket-client")>()
  return {
    ...original,
    createArbitrageSocket: vi.fn(
      (monitorKey: string, handlers: FakeSocket["handlers"]) => {
        const socket: FakeSocket = {
          monitorKey,
          handlers,
          start: vi.fn(),
          stop: vi.fn(),
        }
        socketState.sockets.push(socket)
        return socket
      },
    ),
  }
})

function opportunity(monitorKey: string): ArbitrageOpportunity {
  const generatedAt = "2026-09-10T12:00:00Z"
  return {
    id: `${monitorKey}:${generatedAt}`,
    monitorType: "regular",
    monitorKey,
    marketLabel: "Fed raises rates",
    underlying: null,
    intervalSeconds: null,
    side: "LONG",
    generatedAt,
    signals: [
      {
        contractId: "polymarket:yes",
        venueId: "polymarket",
        direction: "buy",
        quantity: 1,
        limitPrice: 0.4,
        fairProbability: 0.6,
        edge: 0.05,
        strategyId: "long",
        generatedAt,
      },
      {
        contractId: "predict:no",
        venueId: "predict",
        direction: "buy",
        quantity: 1,
        limitPrice: 0.5,
        fairProbability: 0.5,
        edge: 0.05,
        strategyId: "long",
        generatedAt,
      },
    ],
  }
}

function Probe() {
  const stream = useArbitrageStream()
  return (
    <div>
      <span>{stream.connectionStatus}</span>
      <span>{stream.signalCount} received</span>
      {stream.opportunities.map((item) => (
        <span key={item.id}>{item.marketLabel}</span>
      ))}
    </div>
  )
}

describe("ArbitrageStreamProvider", () => {
  beforeEach(() => {
    socketState.sockets.length = 0
    socketState.regularMonitorKeys.length = 0
  })

  test("opens sockets only for explicitly selected regular events", async () => {
    socketState.regularMonitorKeys.push(
      "regular:fed-rate-hike",
      "regular:fomc-dissent",
    )
    render(
      <ArbitrageStreamProvider>
        <Probe />
      </ArbitrageStreamProvider>,
    )

    await waitFor(() => expect(socketState.sockets).toHaveLength(2))
    expect(socketState.sockets.map((socket) => socket.monitorKey)).toEqual([
      "regular:fed-rate-hike",
      "regular:fomc-dissent",
    ])

    act(() => {
      socketState.sockets.forEach((socket) =>
        socket.handlers.onStatus("connected"),
      )
      socketState.sockets[0]!.handlers.onMessage(
        opportunity("regular:fed-rate-hike"),
      )
    })

    expect(screen.getByText("connected")).toBeInTheDocument()
    expect(screen.getByText("1 received")).toBeInTheDocument()
    expect(screen.getByText("Fed raises rates")).toBeInTheDocument()
  })

  test("does not open recurring crypto sockets when no event is selected", async () => {
    render(
      <ArbitrageStreamProvider>
        <Probe />
      </ArbitrageStreamProvider>,
    )

    await waitFor(() => expect(screen.getByText("disconnected")).toBeInTheDocument())
    expect(socketState.sockets).toHaveLength(0)
  })

  test("keeps selected sockets alive while routed content changes", async () => {
    socketState.regularMonitorKeys.push("regular:fed-rate-hike")
    const view = render(
      <ArbitrageStreamProvider>
        <Probe />
      </ArbitrageStreamProvider>,
    )
    await waitFor(() => expect(socketState.sockets).toHaveLength(1))

    view.rerender(
      <ArbitrageStreamProvider>
        <div>Another route</div>
      </ArbitrageStreamProvider>,
    )

    expect(screen.getByText("Another route")).toBeInTheDocument()
    expect(socketState.sockets[0]!.stop).not.toHaveBeenCalled()
  })
})
