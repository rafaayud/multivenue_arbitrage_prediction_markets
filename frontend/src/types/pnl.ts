export type TimeRange = "1D" | "1W" | "1M" | "3M" | "YTD" | "1Y" | "ALL"
export type PnlView = "venue_account" | "bot_ledger"

export interface PnlPoint {
  observed_at: string
  net_pnl_usd: string
}

export interface PerformanceView {
  scope_label: string
  methodology: string
  requested_range: TimeRange
  effective_range: TimeRange
  range_supported: boolean
  methodology_note: string | null
  summary: {
    realized: string | null
    unrealized: string | null
    total: string | null
    fees: string | null
    gas: string | null
    return_pct: string | null
  }
  series: PnlPoint[]
  comparability: {
    comparable: boolean
    confidence: "high" | "medium" | "low"
    notes: string[]
  }
  partial: boolean
  last_updated: string
  quality_flags: string[]
}

export interface LedgerPosition {
  position_id: string
  contract_id: string
  side: string
  quantity: string
  average_entry_price: string | null
  current_price: string | null
  realized_pnl: string
  fee_settlement_amount: string | null
  fee_settlement_currency: string | null
  quality_flags: string[]
  portfolio_id: string | null
  venue_id: string
  opened_at: string | null
  updated_at: string | null
}

export interface PnlDashboard {
  generated_at: string
  portfolio_performance: {
    default_view: "bot_ledger"
    selected_view: PnlView
    selected_range: TimeRange
    available_ranges: TimeRange[]
    venue_account: PerformanceView
    bot_ledger: PerformanceView
  }
  terminal_executions: {
    gross_pnl_usd: string
    fees_usd: string
    net_pnl_usd: string
    priced_terminal_executions: number
    unpriced_terminal_executions: number
  }
  reconciliation: {
    internal_net_pnl_usd: string | null
    venue_reported_net_pnl_usd: string | null
    difference_usd: string | null
    notes: string[]
    internal_venues: Array<{
      venue_id: string
      realized_pnl_usd: string
      unrealized_pnl_usd: string | null
      fees_usd: string | null
      total_pnl_usd: string | null
      partial: boolean
    }>
  }
  venue_health: Array<{
    venue_id: string
    observed_at: string | null
    stale: boolean
    scope: string | null
    missing_fees: boolean
    status: "operational" | "degraded" | "unavailable"
    notes: string[]
  }>
  positions: LedgerPosition[]
}
