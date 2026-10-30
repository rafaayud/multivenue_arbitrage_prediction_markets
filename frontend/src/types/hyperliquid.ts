/** Public outcome data only; these responses cannot enable trading. */
export interface HyperliquidMarket {
  market_id: string
  title: string
  status: string
  closes_at: string | null
  rules: string | null
  source: string | null
  yes_contract_id: string
  no_contract_id: string
}

export interface HyperliquidCatalog {
  enabled: boolean
  network: "mainnet" | "testnet" | null
  trading_enabled: false
  markets: HyperliquidMarket[]
}

/** Levels retain exact decimal strings as [price, token quantity]. */
export interface HyperliquidBook {
  contract_id: string
  outcome: "YES" | "NO"
  source_at: string | null
  bids: [string, string][]
  asks: [string, string][]
}

export interface HyperliquidBooks {
  network: "mainnet" | "testnet"
  observed_at: string
  trading_enabled: false
  books: HyperliquidBook[]
}
