import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { beforeEach, describe, expect, test, vi } from "vitest"

import { DashboardPage } from "@/components/dashboard/dashboard-page"

const state = vi.hoisted(() => ({
  unmonitor: vi.fn().mockResolvedValue(undefined),
  run: null as null | {
    status: string
    short_market_keys: string[]
  },
  regularMarkets: [] as Array<{
    monitor_key: string
    pair_count: number
    markets: Array<{ venue_id: string; title: string }>
    pairs: []
  }>,
}))

vi.mock("@/components/runtime/trading-control", () => ({
  TradingControl: ({ shortMarketKeys }: { shortMarketKeys: string[] }) => (
    <output data-testid="short-market-keys">{shortMarketKeys.join(",")}</output>
  ),
}))

vi.mock("@/components/signals/opportunity-table", () => ({
  OpportunityTable: ({ opportunities }: { opportunities: unknown[] }) => (
    <output>{opportunities.length} event opportunities</output>
  ),
}))

vi.mock("@/features/arbitrage/arbitrage-stream-provider", () => ({
  useArbitrageStream: () => ({
    connectionStatus: "connected",
    connectionError: null,
    opportunities: [
      { monitorType: "cycle", id: "btc" },
      { monitorType: "regular", id: "fed" },
    ],
    signalCount: 1,
    lastReceivedAt: null,
  }),
}))

vi.mock("@/features/runtime/use-runtime", () => ({
  useRuntime: () => ({
    runtime: {
      running: true,
      regular_markets: state.regularMarkets,
    },
    requestState: "success",
    error: null,
    unmonitor: state.unmonitor,
  }),
}))

vi.mock("@/features/system/use-backend-status", () => ({
  useBackendStatus: () => ({
    health: true,
    ready: true,
    loading: false,
    error: null,
  }),
}))

vi.mock("@/features/trading/use-trading-run", () => ({
  useTradingRun: () => ({
    run: state.run,
    requestState: "success",
    error: null,
    enable: vi.fn(),
    disable: vi.fn(),
  }),
}))

describe("DashboardPage", () => {
  beforeEach(() => {
    state.unmonitor.mockClear()
    state.run = null
    state.regularMarkets = []
  })

  test("shows only explicitly selected event opportunities", () => {
    render(<DashboardPage />)

    expect(
      screen.getByRole("heading", { name: "Policy arbitrage monitor" }),
    ).toBeInTheDocument()
    expect(screen.getByText("1 event opportunities")).toBeInTheDocument()
    expect(screen.queryByText(/BTC|ETH|BNB/)).not.toBeInTheDocument()
  })

  test("disconnects one selected multi-venue event", async () => {
    const user = userEvent.setup()
    state.regularMarkets = [
      {
        monitor_key: "regular:fed-rate-decision",
        pair_count: 1,
        markets: [
          { venue_id: "POLYMARKET", title: "Fed raises rates" },
          { venue_id: "PREDICT", title: "Fed raises rates" },
        ],
        pairs: [],
      },
    ]
    render(<DashboardPage />)

    expect(screen.getByText("POLYMARKET")).toBeInTheDocument()
    expect(screen.getByText("PREDICT")).toBeInTheDocument()
    await user
      .click(screen.getByRole("button", { name: "Disconnect policy event" }))

    expect(state.unmonitor).toHaveBeenCalledWith("regular:fed-rate-decision")
  })

  test("passes a selected Fed market to covered-short trading", async () => {
    const user = userEvent.setup()
    state.regularMarkets = [
      {
        monitor_key: "regular:fed-rate-decision",
        pair_count: 1,
        markets: [
          { venue_id: "POLYMARKET", title: "Fed raises rates" },
          { venue_id: "PREDICT", title: "Fed raises rates" },
        ],
        pairs: [],
      },
    ]
    render(<DashboardPage />)

    await user.click(
      screen.getByRole("button", {
        name: "Enable short arbitrage for Fed raises rates",
      }),
    )

    expect(screen.getByTestId("short-market-keys")).toHaveTextContent(
      "regular:fed-rate-decision",
    )
  })
})
