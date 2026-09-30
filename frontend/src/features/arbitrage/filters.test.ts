import { describe, expect, test } from "vitest"

import {
  filterOpportunities,
  type ArbitrageFilters,
} from "@/features/arbitrage/filters"
import type { ArbitrageOpportunity } from "@/types/arbitrage"

function opportunity(
  underlying: "BTC" | "ETH",
  intervalSeconds: 300 | 900,
  side: "LONG" | "SHORT",
): ArbitrageOpportunity {
  const direction = side === "LONG" ? "buy" : "sell"
  const leg = {
    contractId: `${underlying}:${side}`,
    venueId: "venue",
    direction,
    quantity: 1,
    limitPrice: 0.4,
    fairProbability: 0.5,
    edge: 0.05,
    strategyId: "test",
    generatedAt: "2026-07-23T14:30:00Z",
  } as const
  return {
    id: `${underlying}:${intervalSeconds}:${side}`,
    monitorType: "cycle",
    monitorKey: `cycle:${underlying}:${intervalSeconds}`,
    marketLabel: underlying,
    underlying,
    intervalSeconds,
    side,
    signals: [leg, { ...leg, contractId: `${leg.contractId}:other` }],
    generatedAt: leg.generatedAt,
  }
}

describe("filterOpportunities", () => {
  test("combines underlying, interval and side filters", () => {
    const opportunities = [
      opportunity("BTC", 300, "LONG"),
      opportunity("BTC", 900, "SHORT"),
      opportunity("ETH", 300, "LONG"),
    ]
    const filters: ArbitrageFilters = {
      underlying: "BTC",
      interval: 300,
      side: "LONG",
    }

    expect(filterOpportunities(opportunities, filters).map(({ id }) => id)).toEqual([
      "BTC:300:LONG",
    ])
  })
})
