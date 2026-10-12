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

export interface ApiFailure {
  status: number
  detail: string
}
