import type {
  ArbitrageOpportunity,
  IntervalSeconds,
  OpportunitySide,
  Underlying,
} from "@/types/arbitrage"

/** User-selectable constraints applied to received opportunities. */
export interface ArbitrageFilters {
  underlying: Underlying | "ALL"
  interval: IntervalSeconds | "ALL"
  side: Exclude<OpportunitySide, "MIXED"> | "ALL"
}

/** Return opportunities that satisfy every active dashboard filter. */
export function filterOpportunities(
  opportunities: ArbitrageOpportunity[],
  filters: ArbitrageFilters,
): ArbitrageOpportunity[] {
  return opportunities.filter(
    (opportunity) =>
      (filters.underlying === "ALL" ||
        opportunity.underlying === filters.underlying) &&
      (filters.interval === "ALL" ||
        opportunity.intervalSeconds === filters.interval) &&
      (filters.side === "ALL" || opportunity.side === filters.side),
  )
}
