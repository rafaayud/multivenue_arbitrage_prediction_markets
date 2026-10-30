import { act, fireEvent, render, screen, waitFor } from "@testing-library/react"
import { afterEach, expect, test, vi } from "vitest"
import { HyperliquidOutcomes } from "@/components/candidates/hyperliquid-outcomes"
import { apiClient } from "@/lib/api-client"
import type { HyperliquidBooks } from "@/types/hyperliquid"

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })

test("shows a disabled integration without requesting books", async () => {
  vi.spyOn(apiClient, "hyperliquidMarkets").mockResolvedValue({ enabled: false, network: null, trading_enabled: false, markets: [] })
  const books = vi.spyOn(apiClient, "hyperliquidBooks")
  render(<HyperliquidOutcomes />)
  expect(await screen.findByText(/disabled on this server/)).toBeInTheDocument()
  expect(books).not.toHaveBeenCalled()
  expect(screen.queryByRole("button", { name: /trade|monitor/i })).not.toBeInTheDocument()
})

test("selects a question and displays both native books with trading disabled", async () => {
  vi.spyOn(apiClient, "hyperliquidMarkets").mockResolvedValue({
    enabled: true, network: "mainnet", trading_enabled: false,
    markets: [{ market_id: "3", title: "FOMC December: Increase", status: "active", closes_at: "2099-12-31T00:00:00Z", rules: "Use the official decision.", source: "Federal Reserve", yes_contract_id: "hyperliquid:3:yes", no_contract_id: "hyperliquid:3:no" }],
  })
  const snapshot: HyperliquidBooks = {
    network: "mainnet", observed_at: "2026-10-30T18:00:00Z", trading_enabled: false,
    books: [
      { contract_id: "hyperliquid:3:yes", outcome: "YES", source_at: "2026-10-30T18:00:00Z", bids: [["0.11", "50"]], asks: [["0.12", "25"]] },
      { contract_id: "hyperliquid:3:no", outcome: "NO", source_at: "2026-10-30T17:59:00Z", bids: [["0.25", "100"]], asks: [] },
    ],
  }
  const books = vi.spyOn(apiClient, "hyperliquidBooks").mockResolvedValue(snapshot)
  const { unmount } = render(<HyperliquidOutcomes />)
  fireEvent.click(await screen.findByRole("button", { name: /FOMC December/ }))
  expect(await screen.findByRole("table", { name: "YES bids" })).toHaveTextContent("0.11000")
  expect(screen.getByRole("table", { name: "NO bids" })).toHaveTextContent("0.25000")
  expect(screen.getByText("Trading disabled")).toBeInTheDocument()
  expect(screen.queryByRole("button", { name: /trade|monitor/i })).not.toBeInTheDocument()
  await waitFor(() => expect(books).toHaveBeenCalledOnce())
  vi.useFakeTimers()
  const signal = books.mock.calls.at(0)?.[1]
  unmount()
  expect(signal?.aborted).toBe(true)
  books.mockResolvedValueOnce(snapshot).mockRejectedValue(new Error("Upstream unavailable"))
  render(<HyperliquidOutcomes />)
  await act(async () => { await Promise.resolve() })
  fireEvent.click(screen.getByRole("button", { name: /FOMC December/ }))
  await act(async () => { await Promise.resolve() })
  expect(screen.getByRole("table", { name: "YES bids" })).toHaveTextContent("0.11000")
  await act(async () => { await vi.advanceTimersByTimeAsync(5_000) })
  expect(screen.getByRole("alert")).toHaveTextContent("Upstream unavailable")
  expect(screen.queryByRole("table", { name: "YES bids" })).not.toBeInTheDocument()
})
