import { describe, expect, test } from "vitest"

import { parseArbitrageOpportunity, parseStoredArbitrageOpportunity } from "@/types/arbitrage"

function signalPair(firstGeneratedAt: string, secondGeneratedAt: string) {
  return {
    type: "arbitrage_signal_pair",
    underlying: "BTC",
    interval_seconds: 300,
    signals: [
      {
        contract_id: "poly:yes",
        venue_id: "polymarket",
        direction: "buy",
        quantity: "2",
        limit_price: "0.40",
        fair_probability: "0.55",
        edge: "0.09",
        strategy_id: "long-arbitrage",
        generated_at: firstGeneratedAt,
      },
      {
        contract_id: "limitless:no",
        venue_id: "limitless",
        direction: "buy",
        quantity: "2",
        limit_price: "0.50",
        fair_probability: "0.55",
        edge: "0.09",
        strategy_id: "long-arbitrage",
        generated_at: secondGeneratedAt,
      },
    ],
  }
}

describe("parseArbitrageOpportunity", () => {
  test("parses the real string decimal payload as one two-leg opportunity", () => {
    const opportunity = parseArbitrageOpportunity(
      signalPair("2026-07-23T14:30:00Z", "2026-07-23T14:30:01Z"),
    )

    expect(opportunity.side).toBe("LONG")
    expect(opportunity.signals).toHaveLength(2)
    expect(opportunity.signals[0].limitPrice).toBe(0.4)
    expect(opportunity.generatedAt).toBe("2026-07-23T14:30:01Z")
  })

  test("assigns a new id to each detection timestamp", () => {
    const first = parseArbitrageOpportunity(
      signalPair("2026-07-23T14:30:00Z", "2026-07-23T14:30:01Z"),
    )
    const repeated = parseArbitrageOpportunity(
      signalPair("2026-07-23T14:31:00Z", "2026-07-23T14:31:01Z"),
    )

    expect(repeated.id).not.toBe(first.id)
  })

  test("parses a regular market without cycle fields", () => {
    const payload = signalPair(
      "2026-07-23T14:30:00Z",
      "2026-07-23T14:30:01Z",
    )
    const opportunity = parseArbitrageOpportunity({
      ...payload,
      monitor_type: "regular",
      monitor_key: "regular:market-1",
      market_label: "Will the regular event happen?",
      underlying: null,
      interval_seconds: null,
    })

    expect(opportunity.monitorType).toBe("regular")
    expect(opportunity.marketLabel).toBe("Will the regular event happen?")
    expect(opportunity.intervalSeconds).toBeNull()
  })

  test("rejects malformed messages", () => {
    expect(() =>
      parseArbitrageOpportunity({
        type: "arbitrage_signal_pair",
        underlying: "BTC",
        interval_seconds: 300,
        signals: [],
      }),
    ).toThrow("exactly two legs")
  })

  test.each(["buy", "sell"])("deduplicates saved %s detections against the same live pair", (side) => {
    const timestamp = "2026-10-12T10:37:33.768227Z"
    const payload = signalPair(timestamp, timestamp)
    const signals = payload.signals.map((signal) => ({
      ...signal, direction: side,
      strategy_id: side === "buy" ? "long-arbitrage" : "short-arbitrage",
    }))
    const live = parseArbitrageOpportunity({ ...payload, signals: [...signals].reverse() })
    const saved = parseStoredArbitrageOpportunity({
      monitor_type: "cycle", monitor_key: "cycle:BTC:300", underlying: "BTC", interval_seconds: 300,
      side, left_contract_id: "poly:yes", right_contract_id: "limitless:no",
      left_price: "0.40", right_price: "0.50", quantity: "2", net_edge: "0.09", detected_at: timestamp,
    })
    expect(saved.id).toBe(live.id)
    expect(saved.generatedAt).toBe(timestamp)
    expect(saved.signals[0].edge).toBe(0.09)
    expect(saved.side).toBe(side === "buy" ? "LONG" : "SHORT")
  })
})
