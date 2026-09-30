import { render, screen, within } from "@testing-library/react"
import { beforeEach, describe, expect, test, vi } from "vitest"

import { MarketsPage } from "@/components/markets/markets-page"
import type { RegularMonitoredMarket } from "@/types/runtime"
import type { ContractMatch, MarketMatchesResponse } from "@/types/api"

const state = vi.hoisted(() => ({
  matches: [] as MarketMatchesResponse[],
  regularMarkets: [] as RegularMonitoredMarket[],
}))

vi.mock("@/features/markets/use-market-matches", () => ({
  useMarketMatches: () => ({
    matches: state.matches,
    loading: false,
    error: null,
    updatedAt: null,
  }),
}))

vi.mock("@/features/runtime/use-runtime", () => ({
  useRuntime: () => ({ runtime: { regular_markets: state.regularMarkets } }),
}))

function contract(id: string, venueId: string): ContractMatch {
  return {
    id,
    market_id: `${id}-market`,
    outcome_id: "YES",
    venue_id: venueId,
    symbol: "YES",
  }
}

function pair(id: string): { left: ContractMatch; right: ContractMatch } {
  return {
    left: contract(`${id}-left`, "polymarket"),
    right: contract(`${id}-right`, "limitless"),
  }
}

describe("MarketsPage", () => {
  beforeEach(() => {
    state.matches = []
    state.regularMarkets = []
  })

  test("groups monitored markets and summarizes their routes", () => {
    state.matches = [
      {
        monitor_key: "cycle:NVDA:86400",
        family: "finance",
        underlying: "NVDA",
        interval_seconds: 86400,
        pairs: [pair("cycle")],
      },
    ]
    state.regularMarkets = [
      {
        monitor_key: "regular:nvda-event",
        pair_count: 99,
        markets: [
          { venue_id: "polymarket", title: "Will NVDA close above $200?" },
          { venue_id: "limitless", title: "NVDA close above $200" },
        ],
        pairs: [pair("regular-1"), pair("regular-2")],
      },
    ]

    render(<MarketsPage />)

    expect(screen.getByText("2/2 markets")).toBeInTheDocument()
    expect(screen.getByText("finance · 1")).toBeInTheDocument()
    expect(screen.getByText("regular · 1")).toBeInTheDocument()
    expect(screen.getByText("Will NVDA close above $200?")).toBeInTheDocument()
    expect(screen.getByText("2 routes")).toBeInTheDocument()
    expect(within(screen.getByRole("table")).getAllByRole("row")).toHaveLength(5)
  })
})
