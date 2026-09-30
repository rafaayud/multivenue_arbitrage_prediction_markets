import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { describe, expect, test, vi } from "vitest"

import { PnlPage } from "@/components/pnl/pnl-page"

vi.mock("@/components/execution/execution-status", () => ({
  ExecutionFeedStatus: () => null,
}))

vi.mock("@/features/pnl/use-pnl", () => ({
  usePnl: () => ({
    loading: false,
    error: null,
    updatedAt: null,
    pnl: {
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
    },
  }),
}))

describe("PnlPage", () => {
  test("keeps ledger performance primary and venue differences diagnostic", async () => {
    const user = userEvent.setup()
    render(<PnlPage />)

    expect(screen.getByText("Total PnL")).toBeInTheDocument()
    expect(screen.getAllByText("$3.25").length).toBeGreaterThan(0)
    expect(screen.getByText("Closed execution result")).toBeInTheDocument()
    expect(screen.getByText("$0.6463")).toBeInTheDocument()
    expect(screen.getByText("$0.8637")).toBeInTheDocument()
    expect(screen.getByText("Gas")).toBeInTheDocument()
    expect(screen.getByText("$0.05")).toBeInTheDocument()
    expect(screen.getByRole("img", { name: "Portfolio performance in USD" })).toBeInTheDocument()
    expect(screen.getByText("resolved + unresolved positions")).toBeInTheDocument()
    expect(screen.getByText("predict:42:1")).toBeInTheDocument()
    expect(screen.getAllByText("$0.75").length).toBeGreaterThan(0)

    await user.click(screen.getByRole("button", { name: "Venue account" }))
    expect(screen.getByText("Inconsistent scope")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: "1M" })).toBeDisabled()
  })
})
