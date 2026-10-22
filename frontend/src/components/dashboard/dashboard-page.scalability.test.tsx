import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { beforeEach, describe, expect, test, vi } from "vitest"

import { DashboardPage } from "@/components/dashboard/dashboard-page"
import type { MarketMatchPair } from "@/types/api"
import type { InventorySellPair } from "@/types/trading"

const state = vi.hoisted(() => ({
  unmonitor: vi.fn().mockResolvedValue(undefined),
  run: null as null | {
    id: string
    status: string
    short_market_keys: string[]
    existing_inventory_pairs?: InventorySellPair[]
  },
  regularMarkets: [] as Array<{
    monitor_key: string
    pair_count: number
    markets: Array<{ venue_id: string; title: string }>
    pairs: MarketMatchPair[]
  }>,
}))

vi.mock("@/components/runtime/trading-control", () => ({
  TradingControl: ({ shortMarketKeys, existingInventoryPairs, manualInventoryMarketKeys }: { shortMarketKeys: string[], existingInventoryPairs: InventorySellPair[], manualInventoryMarketKeys: string[] }) => (
    <>
      <output data-testid="short-market-keys">{shortMarketKeys.join(",")}</output>
      <output data-testid="inventory-pairs">{JSON.stringify(existingInventoryPairs)}</output>
      <output data-testid="manual-inventory-markets">{manualInventoryMarketKeys.join(",")}</output>
    </>
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
    localStorage.clear()
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

  test("selects an exact existing-token direction without enabling split mode", async () => {
    const user = userEvent.setup()
    const contract = (venue: string, outcome: string) => ({
      id: `${venue}-${outcome}`, venue_id: venue, market_id: `${venue}-fed`,
      outcome_id: `fed:${outcome}:token`, symbol: null,
    })
    state.regularMarkets = [{
      monitor_key: "regular:fed", pair_count: 2,
      markets: [{ venue_id: "PREDICT", title: "Fed no change" }],
      pairs: [
        { left: contract("PREDICT", "yes"), right: contract("POLYMARKET", "no") },
        { left: contract("PREDICT", "no"), right: contract("POLYMARKET", "yes") },
      ],
    }]
    const view = render(<DashboardPage />)
    await user.click(screen.getByText("Sell existing inventory"))
    const selected = screen.getByRole("checkbox", { name: "YES PREDICT + NO POLYMARKET" })
    await user.click(selected)
    expect(selected).toBeChecked()
    expect(screen.getByRole("checkbox", { name: "NO PREDICT + YES POLYMARKET" })).not.toBeChecked()
    expect(screen.getByTestId("inventory-pairs")).toHaveTextContent('[["POLYMARKET-no","PREDICT-yes"]]')
    expect(screen.getByTestId("short-market-keys")).toBeEmptyDOMElement()
    expect(screen.getByTestId("manual-inventory-markets")).toHaveTextContent("regular:fed")
    expect(screen.getByRole("button", { name: "Enable short arbitrage for Fed no change" })).toBeDisabled()

    await user.click(screen.getByRole("button", { name: "Select all" }))
    expect(screen.getAllByRole("checkbox").every((checkbox) => (checkbox as HTMLInputElement).checked)).toBe(true)
    expect(JSON.parse(screen.getByTestId("inventory-pairs").textContent!)).toHaveLength(2)
    expect(screen.getByTestId("short-market-keys")).toBeEmptyDOMElement()
    await user.click(screen.getByRole("button", { name: "Clear all" }))
    expect(screen.getByTestId("inventory-pairs")).toHaveTextContent("[]")
    expect(screen.getByRole("button", { name: "Enable short arbitrage for Fed no change" })).toBeEnabled()

    state.run = { id: "run-1", status: "running", short_market_keys: [], existing_inventory_pairs: [["POLYMARKET-no", "PREDICT-yes"]] }
    view.rerender(<DashboardPage />)
    expect(selected).toBeChecked()
    expect(selected).toBeDisabled()
    expect(screen.getByRole("checkbox", { name: "NO PREDICT + YES POLYMARKET" })).toBeDisabled()
    expect(screen.getByRole("button", { name: "Select all" })).toBeDisabled()

    state.run = { ...state.run, status: "stopped" }
    view.rerender(<DashboardPage />)
    expect(selected).toBeChecked()
    expect(selected).toBeEnabled()
    view.unmount()
    const restored = render(<DashboardPage />)
    expect(screen.getByRole("checkbox", { name: "YES PREDICT + NO POLYMARKET" })).toBeChecked()
    await user.click(screen.getByRole("checkbox", { name: "YES PREDICT + NO POLYMARKET" }))
    restored.unmount()
    render(<DashboardPage />)
    await user.click(screen.getByText("Sell existing inventory"))
    expect(screen.getByRole("checkbox", { name: "YES PREDICT + NO POLYMARKET" })).not.toBeChecked()
  })
})
