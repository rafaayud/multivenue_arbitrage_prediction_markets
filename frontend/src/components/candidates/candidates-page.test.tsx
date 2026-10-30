import { fireEvent, render, screen } from "@testing-library/react"
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest"

import { CandidatesPage } from "@/components/candidates/candidates-page"
import type { AggDiscoveryView } from "@/features/agg/use-agg-candidates"
import type { ArbitrageCandidate } from "@/types/agg"

const state = vi.hoisted(() => ({
  view: null as AggDiscoveryView | null,
  filters: null as { searchText?: string } | null,
  candidates: [] as ArbitrageCandidate[],
  error: null as string | null,
}))

vi.mock("@/features/agg/use-agg-candidates", () => ({
  useArbitrageCandidates: (
    filters: { searchText?: string },
    view: AggDiscoveryView,
  ) => {
    state.view = view
    state.filters = filters
    return {
      candidates: state.candidates,
      loading: false,
      error: state.error,
      updatedAt: null,
    }
  },
}))

describe("CandidatesPage", () => {
  afterEach(() => vi.unstubAllGlobals())

  beforeEach(() => {
    state.candidates = []
    state.error = null
  })

  test("browses unfiltered markets and can switch to live edges", () => {
    render(<CandidatesPage />)

    expect(state.view).toBe("markets")
    expect(state.filters).toEqual({ searchText: "" })
    expect(screen.getByText("No matching events available")).toBeInTheDocument()

    fireEvent.click(screen.getByRole("button", { name: "Live edges" }))

    expect(state.view).toBe("opportunities")
    expect(screen.getByText("No live edges available")).toBeInTheDocument()
  })

  test("can clear a failing upstream search without restoring the default term", () => {
    state.error = "Upstream HTTP 500"
    render(<CandidatesPage />)
    fireEvent.click(screen.getByRole("button", { name: "Fed" }))
    expect(state.filters).toEqual({ searchText: "Fed" })
    fireEvent.click(screen.getByRole("button", { name: "Browse all markets" }))
    expect(state.filters).toEqual({ searchText: "" })
    expect(screen.getByLabelText("Search candidates")).toHaveValue("")
  })

  test("rounds decimal strings from the API for total and venue volumes", () => {
    state.candidates = [
      {
        market_id: "event",
        title: "Rate decision",
        event_title: "Fed",
        venue_event_id: null,
        return_rate: 0,
        observed_at: "2026-09-30T12:00:00Z",
        starts_at: null,
        ends_at: null,
        volume_usd: "10709335.458994005",
        liquidity_usd: null,
        liquidity_tier: null,
        markets: [
          {
            venue_id: "POLYMARKET",
            market_id: "m1",
            external_market_id: "1",
            yes_outcome_id: "y",
            no_outcome_id: "n",
            title: "Rate decision",
            volume_usd: "3430.326845",
          },
        ],
      },
    ]
    render(<CandidatesPage />)
    expect(screen.getByText("$10,709,335.46")).toBeInTheDocument()
    expect(screen.getByText("Volume $3,430.33")).toBeInTheDocument()
    expect(screen.queryByText(/458994005|326845/)).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Needs match" })).toBeDisabled()
  })

  test("sends Hyperliquid from Monitor without dropping its native outcome id", async () => {
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      monitor_key: "regular:fed", pair_count: 2, venue_ids: ["POLYMARKET", "HYPERLIQUID"],
    }), { status: 200 }))
    vi.stubGlobal("fetch", fetch)
    state.candidates = [{
      market_id: "fed", title: "No change", event_title: "Fed", venue_event_id: null,
      return_rate: 0, observed_at: "2026-10-30T12:00:00Z", starts_at: null, ends_at: null,
      volume_usd: null, liquidity_usd: null, liquidity_tier: null,
      markets: [
        { venue_id: "POLYMARKET", market_id: "poly", external_market_id: "3215007", title: "No change", yes_outcome_id: "y", no_outcome_id: "n", volume_usd: null },
        { venue_id: "HYPERLIQUID", market_id: "hyper", external_market_id: "outcome:19988", title: "No change", yes_outcome_id: "y", no_outcome_id: "n", volume_usd: null },
        { venue_id: "KALSHI", market_id: "kalshi", external_market_id: "KXFEDDECISION-26DEC-H0", title: "No change", yes_outcome_id: "y", no_outcome_id: "n", volume_usd: null },
      ],
    }]
    render(<CandidatesPage />)
    fireEvent.click(screen.getByRole("button", { name: "Monitor" }))
    expect(await screen.findByText(/POLYMARKET \+ HYPERLIQUID.*books only/)).toBeInTheDocument()
    const body = JSON.parse((fetch.mock.calls[0]![1] as RequestInit).body as string)
    expect(body.markets.map((market: { venue_id: string }) => market.venue_id)).toEqual(["POLYMARKET", "HYPERLIQUID"])
    expect(body.markets[1].external_market_id).toBe("outcome:19988")
  })
})
