import type { IntervalSeconds, Underlying } from "@/types/arbitrage"

export interface MessageResponse {
  message: string
}

export interface ContractMatch {
  id: string
  market_id: string
  outcome_id: string
  venue_id: string
  symbol: string | null
}

export interface MarketMatchPair {
  left: ContractMatch
  right: ContractMatch
}

export interface MarketMatchesResponse {
  monitor_key: string
  family: "crypto" | "finance"
  underlying: Underlying
  interval_seconds: IntervalSeconds
  pairs: MarketMatchPair[]
}

export interface ApiFailure {
  status: number
  detail: string
}
