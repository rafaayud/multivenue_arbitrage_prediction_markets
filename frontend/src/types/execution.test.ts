import { describe, expect, test } from "vitest"

import { parseExecutionActivity } from "@/types/execution"

const timestamp = "2026-07-29T18:30:00Z"

describe("parseExecutionActivity", () => {
  test("normalizes persisted orders, fills, positions and recovery state", () => {
    const snapshot = parseExecutionActivity({
      type: "execution_activity_snapshot",
      generated_at: timestamp,
      trading_fees_usd: "0.03",
      gas_usd: "0.004",
      orders: [
        {
          status: "filled",
          contract_id: "polymarket:yes",
          side: "buy",
          quantity: "2",
          order_type: "limit",
          client_order_id: "client-1",
          order_id: "order-1",
          limit_price: "0.40",
          filled_quantity: "2",
          average_price: "0.39",
          created_at: timestamp,
          updated_at: timestamp,
        },
      ],
      trades: [
        {
          trade_id: "trade-1",
          order_id: "order-1",
          client_order_id: "client-1",
          contract_id: "polymarket:yes",
          side: "buy",
          quantity: "2",
          price: "0.39",
          executed_at: timestamp,
          portfolio_id: "portfolio-1",
          strategy_id: "long-arbitrage",
          fee_amount: "0.01",
          fee_currency: "USDC",
          fee_settlement_amount: "0.01",
          fee_settlement_currency: "USD",
        },
      ],
      positions: [
        {
          position_id: "position-1",
          contract_id: "polymarket:yes",
          side: "long",
          quantity: "2",
          average_entry_price: "0.39",
          portfolio_id: "portfolio-1",
          opened_at: timestamp,
          updated_at: timestamp,
        },
      ],
      journals: [
        {
          execution_id: "execution-1",
          monitor_type: "cycle",
          monitor_key: "cycle:BTC:300",
          underlying: "BTC",
          interval_seconds: 300,
          status: "completed",
          leg1: {
            venue_id: "POLYMARKET",
            contract_id: "polymarket:yes",
            side: "buy",
            quantity: "2",
            limit_price: "0.40",
            client_order_id: "client-1",
            order_id: "order-1",
            filled_quantity: "2",
            average_fill_price: "0.39",
            fee_amount: "0.01",
            fee_currency: "USDC",
            fee_settlement_amount: "0.01",
            fee_settlement_currency: "USD",
          },
          leg2: {
            venue_id: "LIMITLESS",
            contract_id: "limitless:no",
            side: "buy",
            quantity: "2",
            limit_price: "0.45",
            client_order_id: "client-2",
            order_id: "order-2",
            filled_quantity: "2",
            average_fill_price: "0.45",
            fee_amount: "0.02",
            fee_currency: "OUTCOME_TOKEN",
            fee_settlement_amount: "0.02",
            fee_settlement_currency: "USD",
          },
          residual_quantity: "0",
          gross_locked_pnl_usd: "0.32",
          total_fee_settlement_cost_usd: "0.03",
          net_locked_pnl_usd: "0.29",
          manual_resolution: {
            execution_id: "execution-1",
            method: "settlement",
            venue_id: "POLYMARKET",
            contract_id: "polymarket:yes",
            side: "sell",
            quantity: "2",
            price: "1",
            fee_amount_usd: "0",
            executed_at: timestamp,
            external_reference: "claim-0x123",
          },
          latency_trace: {
            execution_id: "execution-1",
            outcome: "terminal",
            error: null,
            older_book_role: "primary",
            stages: {
              book_arrival_skew_ms: 12,
              newest_book_to_plan_ms: 1,
              plan_to_dispatcher_ms: 2,
              dispatcher_prepare_ms: 40,
              prepare_to_guard_ms: 1,
              guard_ms: 0.1,
              guard_to_both_submits_ms: 0.2,
            },
            legs: [
              {
                role: "primary",
                venue: "POLYMARKET",
                book_age_at_plan_ms: 13,
                book_age_at_guard_ms: 56,
                book_replaced_before_guard: false,
                prepare_ms: 39,
                watch_ms: 2,
                adapter_prepare_ms: 30,
                journal_append_ms: 6,
                guard_to_submit_ms: 0.1,
                submit_to_ack_ms: 23.1,
                ack_to_first_fill_ms: 0,
                ack_to_terminal_ms: 1,
              },
              {
                role: "hedge",
                venue: "LIMITLESS",
                book_age_at_plan_ms: 1,
                book_age_at_guard_ms: 44,
                book_replaced_before_guard: false,
                prepare_ms: 36.7,
                watch_ms: 3,
                adapter_prepare_ms: 29,
                journal_append_ms: 4,
                guard_to_submit_ms: 0.2,
                submit_to_ack_ms: 36.7,
                ack_to_first_fill_ms: 0,
                ack_to_terminal_ms: 2,
              },
            ],
          },
          portfolio_id: "portfolio-1",
          strategy_id: "long-arbitrage",
          last_error: null,
          created_at: timestamp,
          updated_at: timestamp,
        },
      ],
      recoveries: [
        {
          recovery_id: "recovery-1",
          execution_id: "execution-1",
          route: "unwind_excess",
          venue_id: "POLYMARKET",
          contract_id: "polymarket:yes",
          side: "sell",
          quantity: "1",
          filled_quantity: "1",
          limit_price: "0.38",
          average_price: "0.39",
          source_contract_id: "polymarket:yes",
          source_side: "buy",
          source_price: "0.40",
          source_fee_amount: "0.01",
          source_fee_currency: "USD",
          estimated_vwap: "0.38",
          estimated_recovery_fee_amount: "0.01",
          estimated_recovery_fee_currency: "USD",
          estimated_gross_result: "-0.02",
          estimated_net_result: "-0.04",
          recovery_fee_amount: "0.01",
          recovery_fee_currency: "USD",
          actual_gross_result: "-0.01",
          actual_net_result: "-0.03",
          portfolio_id: "portfolio-1",
          strategy_id: "long-arbitrage",
          status: "resolved",
          attempts: 1,
          client_order_id: "recovery-client",
          order_id: "recovery-order",
          last_error: null,
          created_at: timestamp,
          updated_at: timestamp,
        },
      ],
    })

    expect(snapshot.orders[0]?.filledQuantity).toBe(2)
    expect(snapshot.tradingFeesUsd).toBe(0.03)
    expect(snapshot.gasUsd).toBe(0.004)
    expect(snapshot.trades[0]?.feeAmount).toBe(0.01)
    expect(snapshot.trades[0]?.feeSettlementCurrency).toBe("USD")
    expect(snapshot.positions[0]?.averageEntryPrice).toBe(0.39)
    expect(snapshot.journals[0]?.leg2.venueId).toBe("LIMITLESS")
    expect(snapshot.journals[0]?.monitorKey).toBe("cycle:BTC:300")
    expect(snapshot.journals[0]?.netLockedPnlUsd).toBe(0.29)
    expect(snapshot.journals[0]?.manualResolution?.method).toBe("settlement")
    expect(snapshot.journals[0]?.manualResolution?.price).toBe(1)
    expect(snapshot.journals[0]?.latencyTrace?.legs[0]?.adapter_prepare_ms).toBe(30)
    expect(snapshot.recoveries[0]?.attempts).toBe(1)
    expect(snapshot.recoveries[0]?.actualNetResult).toBe(-0.03)
  })

  test("rejects a non-execution message", () => {
    expect(() =>
      parseExecutionActivity({ type: "arbitrage_signal_pair" }),
    ).toThrow("message is not an execution activity snapshot")
  })
})
