/// <reference types="node" />

import { fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import { env } from "node:process"
import { afterEach, expect, test, vi } from "vitest"
import { CandidatesPage } from "@/components/candidates/candidates-page"
import { apiClient } from "@/lib/api-client"
import type { HyperliquidBooks } from "@/types/hyperliquid"

const baseUrl = env.HYPERLIQUID_LIVE_BASE_URL

// Keep this public-data check independent of the user's configured AGG service.
vi.mock("@/features/agg/use-agg-candidates", () => ({
  useArbitrageCandidates: () => ({ candidates: [], loading: false, error: null, updatedAt: null }),
}))

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals() })

test.skipIf(!baseUrl)("renders and refreshes live native books through the deployed frontend proxy", async () => {
  const nativeFetch = globalThis.fetch
  vi.stubGlobal("fetch", (input: RequestInfo | URL, init?: RequestInit) =>
    nativeFetch(new URL(String(input), baseUrl), init))
  const fetchBooks = apiClient.hyperliquidBooks.bind(apiClient)
  const received: HyperliquidBooks[] = []
  vi.spyOn(apiClient, "hyperliquidBooks").mockImplementation(async (...args) => {
    const response = await fetchBooks(...args)
    received.push(response)
    return response
  })
  render(<CandidatesPage />)
  fireEvent.click(screen.getByRole("button", { name: "Hyperliquid outcomes" }))
  expect(screen.getByText("Trading disabled")).toBeInTheDocument()
  fireEvent.change(screen.getByRole("searchbox", { name: "Search Hyperliquid outcomes" }), { target: { value: "Fed" } })
  fireEvent.click(screen.getByRole("button", { name: "Search outcomes" }))
  const market = await screen.findByRole("button", { name: /Federal Open Market Committee.*Increase/ }, { timeout: 20_000 })
  fireEvent.click(market)

  await waitFor(() => expect(received.length).toBeGreaterThan(0), { timeout: 20_000 })
  const initial = received.at(0)!
  expect(initial.network).toBe("mainnet")
  expect(initial.trading_enabled).toBe(false)
  expect(initial.books.map((book) => book.outcome)).toEqual(["YES", "NO"])
  for (const book of initial.books) {
    expect(book.source_at).toBeTruthy()
    expect(book.bids.length).toBeGreaterThan(0)
    expect(book.asks.length).toBeGreaterThan(0)
    for (const side of ["bids", "asks"] as const) {
      const table = screen.getByRole("table", { name: `${book.outcome} ${side}` })
      const [price, quantity] = book[side][0]!
      const row = within(table).getAllByRole("row")[1]!
      expect(row).toHaveTextContent(Number(price).toFixed(5))
      expect(row).toHaveTextContent(Number(quantity).toLocaleString("en-US", { maximumFractionDigits: 2 }))
    }
  }
  await waitFor(() => expect(received.length).toBeGreaterThanOrEqual(2), { timeout: 20_000 })
  expect(Date.parse(received.at(-1)!.observed_at)).toBeGreaterThan(Date.parse(initial.observed_at))
  expect(screen.queryByRole("button", { name: /activate trading|monitor/i })).not.toBeInTheDocument()
}, 60_000)
