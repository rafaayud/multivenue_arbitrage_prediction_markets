import type { InventorySellPair, TradingRunSettings } from "@/types/trading"

const STORAGE_KEY = "pm-trading-preferences-v1"
const LIMIT_FIELDS = [
  "max_arbitrages", "max_concurrent_arbitrages", "polymarket_max_notional",
  "limitless_max_notional", "predict_max_notional", "min_net_edge",
  "cost_buffer", "max_recovery_loss",
] as const

/** Keep only reusable limits and selections; credentials and consent are excluded. */
function preferences(value: unknown): Partial<TradingRunSettings> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {}
  const input = value as Record<string, unknown>
  const saved: Partial<TradingRunSettings> = {}
  for (const field of LIMIT_FIELDS) {
    const amount = input[field]
    if (typeof amount === "number" && Number.isFinite(amount)) saved[field] = amount
  }
  if (Array.isArray(input.short_market_keys)) {
    saved.short_market_keys = input.short_market_keys.filter(
      (key): key is string => typeof key === "string" && key.length > 0,
    )
  }
  if (Array.isArray(input.existing_inventory_pairs)) {
    saved.existing_inventory_pairs = input.existing_inventory_pairs.filter(
      (pair): pair is InventorySellPair => Array.isArray(pair) && pair.length === 2
        && pair.every((id) => typeof id === "string" && id.length > 0),
    )
  }
  return saved
}

/** Restore browser preferences, falling back when storage is unavailable or malformed. */
export function readTradingPreferences(): Partial<TradingRunSettings> {
  try {
    return preferences(JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}"))
  } catch {
    return {}
  }
}

/** Remember limits or selections without persisting permission to place orders. */
export function saveTradingPreferences(update: Partial<TradingRunSettings>): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(preferences({
      ...readTradingPreferences(), ...update,
    })))
  } catch {
    // Blocked browser storage must not interrupt trading controls.
  }
}
