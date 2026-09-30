export type TradingRunStatus =
  | "preparing"
  | "running"
  | "stopping"
  | "stopped"
  | "completed"
  | "failed"

export interface TradingRun {
  id: string
  status: TradingRunStatus
  started_at: string
  finished_at: string | null
  error: string | null
  short_market_keys: string[]
}

export interface TradingSession {
  authenticated: boolean
}

export interface TradingRunSettings {
  allow_degraded_venues?: boolean
  underlyings: string[]
  intervals_seconds: number[]
  max_arbitrages: number
  max_concurrent_arbitrages: number
  polymarket_max_notional: number
  limitless_max_notional: number
  predict_max_notional: number
  predict_limit_slippage_ticks: number
  predict_use_edge_budget?: boolean
  min_net_edge: number
  cost_buffer: number
  max_recovery_loss: number
  short_market_keys: string[]
}
