import type {
  ArbitrageOpportunity,
  OpportunitySide,
} from "@/types/arbitrage"

/** User-selectable constraints applied to received opportunities. */
export interface ArbitrageFilters {
  side: Exclude<OpportunitySide, "MIXED"> | "ALL"
}

/** Return opportunities that satisfy every active dashboard filter. */
export function filterOpportunities(
  opportunities: ArbitrageOpportunity[],
  filters: ArbitrageFilters,
): ArbitrageOpportunity[] {
  return opportunities.filter(
    (opportunity) =>
      (filters.side === "ALL" || opportunity.side === filters.side),
  )
}
