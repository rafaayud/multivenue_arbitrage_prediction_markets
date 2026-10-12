import { fireEvent, render, screen, within } from "@testing-library/react"
import { describe, expect, test, vi } from "vitest"

import { SignalsPage } from "@/components/signals/signals-page"
import type { ArbitrageOpportunity } from "@/types/arbitrage"

const state = vi.hoisted(() => ({
  opportunities: [] as ArbitrageOpportunity[],
}))

vi.mock("@/features/arbitrage/arbitrage-stream-provider", () => ({
  useArbitrageStream: () => ({
    connectionStatus: "connected",
    opportunities: state.opportunities,
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
  title: string,
  side: "LONG" | "SHORT" = "LONG",
): ArbitrageOpportunity {
  const generatedAt = "2026-07-29T12:00:00Z"
  return {
    id: `${title}:${side}`,
    monitorType: "regular",
    monitorKey: `regular:${title}`,
    marketLabel: title,
    underlying: null,
    intervalSeconds: null,
    side,
    generatedAt,
    signals: [
      {
        contractId: `${title}:left`,
        venueId: "polymarket",
        direction: side === "LONG" ? "buy" : "sell",
        quantity: 1,
        limitPrice: 0.4,
        fairProbability: 0.6,
        edge: 0.05,
        strategyId: "long",
        generatedAt,
      },
      {
        contractId: `${title}:right`,
        venueId: "limitless",
        direction: side === "LONG" ? "buy" : "sell",
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
  test("filters event opportunities by direction without cycle selectors", () => {
    state.opportunities = [opportunity("Election A"), opportunity("Election B", "SHORT")]

    render(<SignalsPage />)

    const trigger = screen.getByRole("combobox", {
      name: "Filter by opportunity type",
    })
    fireEvent.keyDown(trigger, { key: "ArrowDown" })
    const shortOption = screen.getByRole("option", { name: "Short" })
    expect(shortOption).toBeInTheDocument()
    fireEvent.click(shortOption)

    expect(screen.queryByRole("combobox", { name: "Filter by underlying" })).not.toBeInTheDocument()
    expect(screen.queryByRole("combobox", { name: "Filter by interval" })).not.toBeInTheDocument()

    const table = within(screen.getByRole("table"))
    expect(table.getByText("Election B")).toBeInTheDocument()
    expect(table.queryByText("Election A")).not.toBeInTheDocument()
  })
})
