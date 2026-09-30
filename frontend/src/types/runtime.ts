import type { MarketMatchPair } from "@/types/api"

export interface RegularMonitoredMarket {
  monitor_key: string
  pair_count: number
  markets: Array<{
    venue_id: string
    title: string
  }>
  pairs: MarketMatchPair[]
}

export interface RuntimeState {
  running: boolean
  trading_enabled?: boolean
  signal_settings?: SignalSettings
  regular_markets?: RegularMonitoredMarket[]
  short_inventory_preparing?: boolean
  prepared_short_market_keys?: string[]
  last_execution_latency?: ExecutionLatencyTrace | null
}

export interface ExecutionLatencyTrace {
  execution_id: string
  outcome:
    | "pending"
    | "guard_rejected"
    | "guard_passed"
    | "submitted"
    | "acknowledged"
    | "terminal"
  error: string | null
  older_book_role: "primary" | "hedge" | null
  stages: {
    book_arrival_skew_ms: number | null
    newest_book_to_plan_ms: number | null
    plan_to_dispatcher_ms: number | null
    dispatcher_prepare_ms: number | null
    prepare_to_guard_ms: number | null
    guard_ms: number | null
    guard_to_both_submits_ms: number | null
  }
  legs: Array<{
    role: "primary" | "hedge"
    venue: string | null
    book_age_at_plan_ms: number | null
    book_age_at_guard_ms: number | null
    book_replaced_before_guard: boolean
    prepare_ms: number | null
    watch_ms: number | null
    adapter_prepare_ms: number | null
    journal_append_ms: number | null
    guard_to_submit_ms: number | null
    submit_to_ack_ms: number | null
    ack_to_first_fill_ms: number | null
    ack_to_terminal_ms: number | null
  }>
}

export interface SignalSettings {
  min_net_edge: number
  cost_buffer: number
}

export type RequestState = "idle" | "loading" | "success" | "error"
