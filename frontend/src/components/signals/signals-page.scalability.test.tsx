import { fireEvent, render, screen, within } from "@testing-library/react"
import { describe, expect, test, vi } from "vitest"

import { SignalsPage } from "@/components/signals/signals-page"
import type { ArbitrageOpportunity, Underlying } from "@/types/arbitrage"

const state = vi.hoisted(() => ({
  opportunities: [] as ArbitrageOpportunity[],
}))

vi.mock("@/features/arbitrage/arbitrage-stream-provider", () => ({
  useArbitrageStream: () => ({
    connectionStatus: "connected",
    opportunities: state.opportunities,
  }),
}))

vi.mock("@/features/markets/use-market-matches", () => ({
  useMarketMatches: () => ({
    matches: [],
    loading: false,
    error: null,
    updatedAt: null,
  }),
}))

vi.mock("@/features/runtime/use-runtime", () => ({
  useRuntime: () => ({
    runtime: {
      running: true,
      trading_enabled: false,
      signal_settings: { min_net_edge: 0, cost_buffer: 0 },
    },
    requestState: "success",
    error: null,
    updateSignalSettings: vi.fn().mockResolvedValue(undefined),
  }),
}))

function opportunity(
  underlying: Underlying,
  intervalSeconds = 86400,
): ArbitrageOpportunity {
  const generatedAt = "2026-07-29T12:00:00Z"
  return {
    id: `${underlying}:${intervalSeconds}`,
    monitorType: "cycle",
    monitorKey: `cycle:${underlying}:${intervalSeconds}`,
    marketLabel: underlying,
    underlying,
    intervalSeconds,
    side: "LONG",
    generatedAt,
    signals: [
      {
        contractId: `${underlying}:left`,
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
        contractId: `${underlying}:right`,
        venueId: "limitless",
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

describe("SignalsPage scalability", () => {
  test("derives asset options from the live signal stream", () => {
    state.opportunities = [opportunity("SOL", 12345), opportunity("BTC")]

    render(<SignalsPage />)

    const trigger = screen.getByRole("combobox", {
      name: "Filter by underlying",
    })
    fireEvent.keyDown(trigger, { key: "ArrowDown" })
    const solOption = screen.getByRole("option", { name: "SOL" })
    expect(solOption).toBeInTheDocument()
    fireEvent.click(solOption)

    const intervalTrigger = screen.getByRole("combobox", {
      name: "Filter by interval",
    })
    fireEvent.keyDown(intervalTrigger, { key: "ArrowDown" })
    expect(screen.getByRole("option", { name: "12345s" })).toBeInTheDocument()
    fireEvent.keyDown(document, { key: "Escape" })

    const table = within(screen.getByRole("table"))
    expect(table.getByText("SOL")).toBeInTheDocument()
    expect(table.queryByText("BTC")).not.toBeInTheDocument()
  })
})
