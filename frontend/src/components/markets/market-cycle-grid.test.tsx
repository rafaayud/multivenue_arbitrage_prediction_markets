import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { describe, expect, test, vi } from "vitest"

import { MarketCycleGrid } from "@/components/markets/market-cycle-grid"
import type { ContractMatch, MarketMatchesResponse } from "@/types/api"

function contract(id: string, venueId: string): ContractMatch {
  return {
    id,
    market_id: `${id}-market`,
    outcome_id: "YES",
    venue_id: venueId,
    symbol: "YES",
  }
}

const matches: MarketMatchesResponse[] = [
  {
    monitor_key: "cycle:BTC:3600",
    family: "crypto",
    underlying: "BTC",
    interval_seconds: 3600,
    pairs: [
      {
        left: contract("btc-left", "polymarket"),
        right: contract("btc-right", "predict"),
      },
    ],
  },
  {
    monitor_key: "cycle:ETH:3600",
    family: "crypto",
    underlying: "ETH",
    interval_seconds: 3600,
    pairs: [],
  },
]

describe("MarketCycleGrid", () => {
  test("selects matched markets and rejects unavailable ones", async () => {
    const user = userEvent.setup()
    const toggle = vi.fn()
    render(
      <MarketCycleGrid
        matches={matches}
        loading={false}
        selectedShortMarketKeys={[]}
        shortSelectionLocked={false}
        onShortMarketToggle={toggle}
        onAllShortMarketsToggle={vi.fn()}
      />,
    )

    await user.click(
      screen.getByRole("button", {
        name: "Enable short arbitrage for BTC 1h",
      }),
    )

    expect(toggle).toHaveBeenCalledWith("cycle:BTC:3600")
    expect(
      screen.getByRole("button", {
        name: "Enable short arbitrage for ETH 1h",
      }),
    ).toBeDisabled()
  })

  test("keeps the active selection visible and locked during trading", () => {
    render(
      <MarketCycleGrid
        matches={matches}
        loading={false}
        selectedShortMarketKeys={["cycle:BTC:3600"]}
        shortSelectionLocked
        onShortMarketToggle={vi.fn()}
        onAllShortMarketsToggle={vi.fn()}
      />,
    )

    const button = screen.getByRole("button", {
      name: "Disable short arbitrage for BTC 1h",
    })
    expect(button).toHaveAttribute("aria-pressed", "true")
    expect(button).toBeDisabled()
  })

  test("toggles all matched markets without including unavailable ones", async () => {
    const user = userEvent.setup()
    const toggleAll = vi.fn()
    const { rerender } = render(
      <MarketCycleGrid
        matches={matches}
        loading={false}
        selectedShortMarketKeys={[]}
        shortSelectionLocked={false}
        onShortMarketToggle={vi.fn()}
        onAllShortMarketsToggle={toggleAll}
      />,
    )

    await user.click(
      screen.getByRole("button", {
        name: "Enable short arbitrage for all matched markets",
      }),
    )
    expect(toggleAll).toHaveBeenCalledWith(["cycle:BTC:3600"])

    rerender(
      <MarketCycleGrid
        matches={matches}
        loading={false}
        selectedShortMarketKeys={["cycle:BTC:3600"]}
        shortSelectionLocked={false}
        onShortMarketToggle={vi.fn()}
        onAllShortMarketsToggle={toggleAll}
      />,
    )
    expect(
      screen.getByRole("button", {
        name: "Disable short arbitrage for all matched markets",
      }),
    ).toHaveTextContent("Disable all")
  })
})
