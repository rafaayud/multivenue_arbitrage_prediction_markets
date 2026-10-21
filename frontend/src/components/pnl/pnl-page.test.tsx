import { render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { beforeEach, describe, expect, test, vi } from "vitest"

import { PnlPage } from "@/components/pnl/pnl-page"
import { PnlChart } from "@/components/pnl/pnl-chart"
import { usePnl } from "@/features/pnl/use-pnl"
import type { PnlDashboard } from "@/types/pnl"

vi.mock("@/components/execution/execution-status", () => ({
  ExecutionFeedStatus: () => null,
}))

vi.mock("@/components/pnl/pnl-chart", () => ({
  PnlChart: vi.fn(() => <div />),
}))

vi.mock("@/features/pnl/use-pnl", () => ({
  usePnl: vi.fn(),
}))

const fixture: PnlDashboard = {
  generated_at: "2026-08-17T08:00:00Z",
  portfolio_performance: {
    default_view: "bot_ledger",
    selected_view: "bot_ledger",
    selected_range: "1M",
    available_ranges: ["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"],
    bot_ledger: {
      scope_label: "Bot ledger, journal ordered",
      methodology: "WAC realized plus marked unrealized PnL.",
      requested_range: "1M",
      effective_range: "1M",
      range_supported: true,
      methodology_note: null,
      summary: { realized: "4.5", unrealized: "-0.5", fees: "0.70", gas: "0.05", total: "3.25", return_pct: null },
      series: [
        { observed_at: "2026-08-16T08:00:00Z", net_pnl_usd: "1" },
        { observed_at: "2026-08-17T08:00:00Z", net_pnl_usd: "3.25" },
      ],
      comparability: { comparable: true, confidence: "high", notes: [] },
      partial: false,
      last_updated: "2026-08-17T08:00:00Z",
      quality_flags: [],
    },
    venue_account: {
      scope_label: "Venue account snapshots",
      methodology: "Venue values are not merged across scopes.",
      requested_range: "1M",
      effective_range: "ALL",
      range_supported: false,
      methodology_note: "1M is unsupported.",
      summary: { realized: null, unrealized: null, fees: null, gas: null, total: null, return_pct: null },
      series: [],
      comparability: { comparable: false, confidence: "low", notes: ["Scopes differ."] },
      partial: true,
      last_updated: "2026-08-17T08:00:00Z",
      quality_flags: ["INCONSISTENT_SCOPE"],
    },
  },
  terminal_executions: {
    gross_pnl_usd: "1.510053",
    fees_usd: "0.86374106",
    net_pnl_usd: "0.64631194",
    priced_terminal_executions: 6,
    unpriced_terminal_executions: 0,
    gas_usd: "0.05",
    net_after_gas_usd: "0.59631194",
    unpriced_inventory_operations: 0,
    net_after_gas_series: [{ observed_at: "2026-08-17T08:00:00Z", net_pnl_usd: "0.59631194" }],
    executions: [{
      execution_id: "execution-1", status: "completed", underlying: "BTC", monitor_key: "cycle:BTC:300",
      updated_at: "2026-08-17T08:00:00Z", gross_locked_pnl_usd: "1.510053",
      total_fee_settlement_cost_usd: "0.86374106", net_locked_pnl_usd: "0.64631194",
      leg1: { venue_id: "POLYMARKET", contract_id: "poly:yes", side: "buy", filled_quantity: "6" },
      leg2: { venue_id: "PREDICT", contract_id: "predict:no", side: "buy", filled_quantity: "6" },
    }],
  },
  reconciliation: {
    internal_net_pnl_usd: "3.25",
    venue_reported_net_pnl_usd: null,
    difference_usd: null,
    notes: ["Scopes differ."],
    internal_venues: [],
  },
  venue_health: [
    {
      venue_id: "PREDICT",
      observed_at: "2026-08-17T08:00:00Z",
      stale: false,
      scope: "resolved + unresolved_positions",
      missing_fees: true,
      status: "operational",
      notes: [],
    },
  ],
  positions: [
    {
      position_id: "PREDICT:bot:predict:42:1",
      contract_id: "predict:42:1",
      venue_id: "PREDICT",
      portfolio_id: "bot",
      side: "short",
      quantity: "10",
      average_entry_price: "0.6",
      current_price: "0.55",
      realized_pnl: "1.5",
      fee_settlement_amount: "0.75",
      fee_settlement_currency: "USD",
      quality_flags: [],
      opened_at: "2026-08-16T08:00:00Z",
      updated_at: "2026-08-17T08:00:00Z",
    },
  ],
}

let pnl: PnlDashboard

beforeEach(() => {
  pnl = structuredClone(fixture)
  vi.mocked(usePnl).mockReturnValue({ pnl, loading: false, error: null, updatedAt: null })
})

describe("PnlPage", () => {
  test("shows execution PnL after costs without waiting for settlement or deducting twice", () => {
    render(<PnlPage />)

    expect(usePnl).toHaveBeenLastCalledWith("bot_ledger", "ALL")
    expect(screen.getByText("Net arbitrage PnL")).toBeInTheDocument()
    expect(screen.getAllByText("$0.5963")).toHaveLength(2)
    expect(screen.queryByText("$3.25")).not.toBeInTheDocument()
    expect(screen.getByText("Trading fees and recorded gas are already deducted.")).toBeInTheDocument()
    expect(screen.getByText(/Counted as soon as both legs complete/)).toBeInTheDocument()
    const breakdown = screen.getByText("How your result adds up").parentElement!
    expect(within(breakdown).getByText("$1.5101")).toBeInTheDocument()
    expect(within(breakdown).getByText("$0.8637")).toBeInTheDocument()
    expect(within(breakdown).getByText("$0.05")).toBeInTheDocument()
    expect(screen.getByText("Locked")).toBeInTheDocument()
    expect(screen.getByText("$0.6463")).toBeInTheDocument()
    expect(vi.mocked(PnlChart).mock.calls.at(-1)?.[0].series).toEqual(pnl.terminal_executions.net_after_gas_series)
    expect(screen.getByText("Tokens held")).toBeInTheDocument()
    expect(screen.getByText("predict:42:1")).toBeInTheDocument()
    expect(screen.queryByText("Realized result")).not.toBeInTheDocument()
    expect(screen.queryByText("Unrealized change")).not.toBeInTheDocument()
  })

  test("keeps unknown costs and excluded executions explicit", () => {
    pnl.terminal_executions.unpriced_terminal_executions = 2
    pnl.terminal_executions.unpriced_inventory_operations = 1
    pnl.terminal_executions.executions[0]!.net_locked_pnl_usd = null
    render(<PnlPage />)

    expect(screen.getByText(/2 executions are excluded/)).toBeInTheDocument()
    expect(screen.getByText(/1 inventory operations have unknown USD costs/)).toBeInTheDocument()
    expect(screen.getByText("Incomplete costs or fills")).toBeInTheDocument()
    expect(screen.queryByText("Recorded costs included")).not.toBeInTheDocument()
    expect(screen.getByText("Incomplete data")).toBeInTheDocument()
    expect(screen.getByText("N/A")).toBeInTheDocument()
  })

  test("shows recovery losses and paginates the execution history", async () => {
    const user = userEvent.setup()
    pnl.terminal_executions.executions = Array.from({ length: 11 }, (_, index) => ({
      ...fixture.terminal_executions.executions[0]!,
      execution_id: String(index), underlying: `Market ${index}`,
      status: index === 10 ? "recovered" : "completed",
      net_locked_pnl_usd: index === 10 ? "-1.5" : "0.64631194",
    }))
    render(<PnlPage />)

    expect(screen.queryByText("Market 10")).not.toBeInTheDocument()
    await user.click(screen.getByRole("button", { name: "Next" }))
    expect(screen.getByText("Market 10")).toBeInTheDocument()
    expect(screen.getByText("Recovered")).toBeInTheDocument()
    expect(screen.getByText("-$1.50")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "Next" })).toBeDisabled()
    await user.click(screen.getByRole("button", { name: "Previous" }))
    expect(screen.getByText("Market 0")).toBeInTheDocument()
  })

  test("does not display a zero result while data is unavailable", () => {
    vi.mocked(usePnl).mockReturnValue({ pnl: null, loading: false, error: "Unavailable", updatedAt: null })
    render(<PnlPage />)

    expect(screen.getByRole("alert")).toHaveTextContent("Unavailable")
    expect(screen.getAllByText("N/A").length).toBeGreaterThan(0)
    expect(screen.queryByText("$0.00")).not.toBeInTheDocument()
  })
})
