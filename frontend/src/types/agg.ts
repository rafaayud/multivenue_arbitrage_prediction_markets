export interface ArbitrageVenueMarket {
  venue_id: string
  market_id: string
  external_market_id: string
  yes_outcome_id: string
  no_outcome_id: string
  title: string | null
  volume_usd: number | null
}

export interface ArbitrageCandidate {
  market_id: string
  title: string
  event_title: string | null
  venue_event_id: string | null
  return_rate: number
  observed_at: string
  starts_at: string | null
  ends_at: string | null
  volume_usd: number | null
  liquidity_usd: number | null
  liquidity_tier: "deep" | "shallow" | null
  markets: ArbitrageVenueMarket[]
}

export interface RegularMarketMonitor {
  monitor_key: string
  pair_count: number
  venue_ids: string[]
}
