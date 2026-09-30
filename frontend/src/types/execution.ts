import type { ExecutionLatencyTrace } from "@/types/runtime"

export interface ExecutionOrder {
  status: string
  contractId: string
  side: string
  quantity: number
  orderType: string
  clientOrderId: string | null
  orderId: string | null
  limitPrice: number | null
  filledQuantity: number
  averagePrice: number | null
  createdAt: string | null
  updatedAt: string | null
}

export interface ExecutionTrade {
  tradeId: string
  orderId: string | null
  clientOrderId: string | null
  contractId: string
  side: string
  quantity: number
  price: number
  executedAt: string
  portfolioId: string | null
  strategyId: string | null
  feeAmount: number | null
  feeCurrency: string | null
  feeSettlementAmount: number | null
  feeSettlementCurrency: string | null
}

export interface ExecutionPosition {
  positionId: string
  contractId: string
  side: string
  quantity: number
  averageEntryPrice: number | null
  portfolioId: string | null
  openedAt: string | null
  updatedAt: string | null
}

export interface ExecutionLeg {
  venueId: string
  contractId: string
  side: string
  quantity: number
  limitPrice: number
  clientOrderId: string
  orderId: string | null
  filledQuantity: number
  averageFillPrice: number | null
  feeAmount: number | null
  feeCurrency: string | null
  feeSettlementAmount: number | null
  feeSettlementCurrency: string | null
}

export interface ManualExecutionResolution {
  executionId: string
  method: "manual_sale" | "settlement"
  venueId: string
  contractId: string
  side: string
  quantity: number
  price: number
  feeAmountUsd: number
  executedAt: string
  externalReference: string | null
}

export interface ManualExecutionResolutionInput {
  method: ManualExecutionResolution["method"]
  price: number
  feeAmountUsd: number
  executedAt: string
  externalReference: string | null
}

export interface ExecutionJournal {
  executionId: string
  monitorType: "cycle" | "regular" | null
  monitorKey: string | null
  underlying: string | null
  intervalSeconds: number | null
  status: string
  leg1: ExecutionLeg
  leg2: ExecutionLeg
  residualQuantity: number
  grossLockedPnlUsd: number | null
  totalFeeSettlementCostUsd: number | null
  netLockedPnlUsd: number | null
  manualResolution: ManualExecutionResolution | null
  latencyTrace: ExecutionLatencyTrace | null
  portfolioId: string | null
  strategyId: string | null
  lastError: string | null
  createdAt: string
  updatedAt: string
}

export interface ExposureRecovery {
  recoveryId: string
  executionId: string | null
  route: string | null
  venueId: string
  contractId: string
  side: string
  quantity: number
  filledQuantity: number
  limitPrice: number
  averagePrice: number | null
  sourceContractId: string | null
  sourceSide: string | null
  sourcePrice: number | null
  sourceFeeAmount: number | null
  sourceFeeCurrency: string | null
  estimatedVwap: number | null
  estimatedRecoveryFeeAmount: number | null
  estimatedRecoveryFeeCurrency: string | null
  estimatedGrossResult: number | null
  estimatedNetResult: number | null
  recoveryFeeAmount: number | null
  recoveryFeeCurrency: string | null
  actualGrossResult: number | null
  actualNetResult: number | null
  portfolioId: string | null
  strategyId: string | null
  status: string
  attempts: number
  clientOrderId: string | null
  orderId: string | null
  lastError: string | null
  createdAt: string
  updatedAt: string
}

export interface ExecutionActivitySnapshot {
  type: "execution_activity_snapshot"
  generatedAt: string
  tradingFeesUsd: number
  gasUsd: number
  orders: ExecutionOrder[]
  trades: ExecutionTrade[]
  positions: ExecutionPosition[]
  journals: ExecutionJournal[]
  recoveries: ExposureRecovery[]
}

type UnknownRecord = Record<string, unknown>

function record(value: unknown, field: string): UnknownRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${field} must be an object`)
  }
  return value as UnknownRecord
}

function stringValue(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${field} must be a non-empty string`)
  }
  return value
}

function nullableString(value: unknown, field: string): string | null {
  return value == null ? null : stringValue(value, field)
}

function decimal(value: unknown, field: string): number {
  if (typeof value !== "string" && typeof value !== "number") {
    throw new Error(`${field} must be a decimal`)
  }
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) {
    throw new Error(`${field} must be finite`)
  }
  return parsed
}

function nullableDecimal(value: unknown, field: string): number | null {
  return value === null ? null : decimal(value, field)
}

function timestamp(value: unknown, field: string): string {
  const parsed = stringValue(value, field)
  if (Number.isNaN(Date.parse(parsed))) {
    throw new Error(`${field} must be a timestamp`)
  }
  return parsed
}

function nullableTimestamp(value: unknown, field: string): string | null {
  return value === null ? null : timestamp(value, field)
}

function list<T>(
  value: unknown,
  field: string,
  parse: (item: unknown) => T,
): T[] {
  if (!Array.isArray(value)) {
    throw new Error(`${field} must be an array`)
  }
  return value.map(parse)
}

function parseOrder(value: unknown): ExecutionOrder {
  const item = record(value, "order")
  return {
    status: stringValue(item.status, "order.status"),
    contractId: stringValue(item.contract_id, "order.contract_id"),
    side: stringValue(item.side, "order.side"),
    quantity: decimal(item.quantity, "order.quantity"),
    orderType: stringValue(item.order_type, "order.order_type"),
    clientOrderId: nullableString(
      item.client_order_id,
      "order.client_order_id",
    ),
    orderId: nullableString(item.order_id, "order.order_id"),
    limitPrice: nullableDecimal(item.limit_price, "order.limit_price"),
    filledQuantity: decimal(
      item.filled_quantity,
      "order.filled_quantity",
    ),
    averagePrice: nullableDecimal(
      item.average_price,
      "order.average_price",
    ),
    createdAt: nullableTimestamp(item.created_at, "order.created_at"),
    updatedAt: nullableTimestamp(item.updated_at, "order.updated_at"),
  }
}

function parseTrade(value: unknown): ExecutionTrade {
  const item = record(value, "trade")
  return {
    tradeId: stringValue(item.trade_id, "trade.trade_id"),
    orderId: nullableString(item.order_id, "trade.order_id"),
    clientOrderId: nullableString(
      item.client_order_id,
      "trade.client_order_id",
    ),
    contractId: stringValue(item.contract_id, "trade.contract_id"),
    side: stringValue(item.side, "trade.side"),
    quantity: decimal(item.quantity, "trade.quantity"),
    price: decimal(item.price, "trade.price"),
    executedAt: timestamp(item.executed_at, "trade.executed_at"),
    portfolioId: nullableString(item.portfolio_id, "trade.portfolio_id"),
    strategyId: nullableString(item.strategy_id, "trade.strategy_id"),
    feeAmount: nullableDecimal(item.fee_amount, "trade.fee_amount"),
    feeCurrency: nullableString(item.fee_currency, "trade.fee_currency"),
    feeSettlementAmount: nullableDecimal(
      item.fee_settlement_amount ?? null,
      "trade.fee_settlement_amount",
    ),
    feeSettlementCurrency: nullableString(
      item.fee_settlement_currency ?? null,
      "trade.fee_settlement_currency",
    ),
  }
}

function parsePosition(value: unknown): ExecutionPosition {
  const item = record(value, "position")
  return {
    positionId: stringValue(item.position_id, "position.position_id"),
    contractId: stringValue(item.contract_id, "position.contract_id"),
    side: stringValue(item.side, "position.side"),
    quantity: decimal(item.quantity, "position.quantity"),
    averageEntryPrice: nullableDecimal(
      item.average_entry_price,
      "position.average_entry_price",
    ),
    portfolioId: nullableString(
      item.portfolio_id,
      "position.portfolio_id",
    ),
    openedAt: nullableTimestamp(item.opened_at, "position.opened_at"),
    updatedAt: nullableTimestamp(item.updated_at, "position.updated_at"),
  }
}

function parseLeg(value: unknown): ExecutionLeg {
  const item = record(value, "execution leg")
  return {
    venueId: stringValue(item.venue_id, "leg.venue_id"),
    contractId: stringValue(item.contract_id, "leg.contract_id"),
    side: stringValue(item.side, "leg.side"),
    quantity: decimal(item.quantity, "leg.quantity"),
    limitPrice: decimal(item.limit_price, "leg.limit_price"),
    clientOrderId: stringValue(
      item.client_order_id,
      "leg.client_order_id",
    ),
    orderId: nullableString(item.order_id, "leg.order_id"),
    filledQuantity: decimal(item.filled_quantity, "leg.filled_quantity"),
    averageFillPrice: nullableDecimal(
      item.average_fill_price ?? null,
      "leg.average_fill_price",
    ),
    feeAmount: nullableDecimal(item.fee_amount ?? null, "leg.fee_amount"),
    feeCurrency: nullableString(item.fee_currency ?? null, "leg.fee_currency"),
    feeSettlementAmount: nullableDecimal(
      item.fee_settlement_amount ?? null,
      "leg.fee_settlement_amount",
    ),
    feeSettlementCurrency: nullableString(
      item.fee_settlement_currency ?? null,
      "leg.fee_settlement_currency",
    ),
  }
}

function parseManualResolution(value: unknown): ManualExecutionResolution {
  const item = record(value, "manual execution resolution")
  if (item.method !== "manual_sale" && item.method !== "settlement") {
    throw new Error("manual execution resolution method is invalid")
  }
  return {
    executionId: stringValue(item.execution_id, "manual resolution.execution_id"),
    method: item.method,
    venueId: stringValue(item.venue_id, "manual resolution.venue_id"),
    contractId: stringValue(item.contract_id, "manual resolution.contract_id"),
    side: stringValue(item.side, "manual resolution.side"),
    quantity: decimal(item.quantity, "manual resolution.quantity"),
    price: decimal(item.price, "manual resolution.price"),
    feeAmountUsd: decimal(
      item.fee_amount_usd,
      "manual resolution.fee_amount_usd",
    ),
    executedAt: timestamp(item.executed_at, "manual resolution.executed_at"),
    externalReference: nullableString(
      item.external_reference,
      "manual resolution.external_reference",
    ),
  }
}

function parseLatencyTrace(value: unknown): ExecutionLatencyTrace {
  const item = record(value, "latency trace")
  const stages = record(item.stages, "latency trace stages")
  const outcome = item.outcome
  if (
    outcome !== "pending" &&
    outcome !== "guard_rejected" &&
    outcome !== "guard_passed" &&
    outcome !== "submitted" &&
    outcome !== "acknowledged" &&
    outcome !== "terminal"
  ) {
    throw new Error("latency trace outcome is invalid")
  }
  const olderBookRole = item.older_book_role
  if (
    olderBookRole !== null &&
    olderBookRole !== "primary" &&
    olderBookRole !== "hedge"
  ) {
    throw new Error("latency trace older_book_role is invalid")
  }
  return {
    execution_id: stringValue(item.execution_id, "latency trace.execution_id"),
    outcome,
    error: nullableString(item.error, "latency trace.error"),
    older_book_role: olderBookRole,
    stages: {
      book_arrival_skew_ms: nullableDecimal(
        stages.book_arrival_skew_ms,
        "latency trace.stages.book_arrival_skew_ms",
      ),
      newest_book_to_plan_ms: nullableDecimal(
        stages.newest_book_to_plan_ms,
        "latency trace.stages.newest_book_to_plan_ms",
      ),
      plan_to_dispatcher_ms: nullableDecimal(
        stages.plan_to_dispatcher_ms,
        "latency trace.stages.plan_to_dispatcher_ms",
      ),
      dispatcher_prepare_ms: nullableDecimal(
        stages.dispatcher_prepare_ms,
        "latency trace.stages.dispatcher_prepare_ms",
      ),
      prepare_to_guard_ms: nullableDecimal(
        stages.prepare_to_guard_ms,
        "latency trace.stages.prepare_to_guard_ms",
      ),
      guard_ms: nullableDecimal(stages.guard_ms, "latency trace.stages.guard_ms"),
      guard_to_both_submits_ms: nullableDecimal(
        stages.guard_to_both_submits_ms,
        "latency trace.stages.guard_to_both_submits_ms",
      ),
    },
    legs: list(item.legs, "latency trace.legs", (value) => {
      const leg = record(value, "latency trace leg")
      const role = leg.role
      if (role !== "primary" && role !== "hedge") {
        throw new Error("latency trace leg role is invalid")
      }
      if (typeof leg.book_replaced_before_guard !== "boolean") {
        throw new Error("latency trace leg book replacement flag is invalid")
      }
      return {
        role,
        venue: nullableString(leg.venue, "latency trace leg.venue"),
        book_age_at_plan_ms: nullableDecimal(
          leg.book_age_at_plan_ms,
          "latency trace leg.book_age_at_plan_ms",
        ),
        book_age_at_guard_ms: nullableDecimal(
          leg.book_age_at_guard_ms,
          "latency trace leg.book_age_at_guard_ms",
        ),
        book_replaced_before_guard: leg.book_replaced_before_guard,
        prepare_ms: nullableDecimal(
          leg.prepare_ms,
          "latency trace leg.prepare_ms",
        ),
        watch_ms: nullableDecimal(
          leg.watch_ms ?? null,
          "latency trace leg.watch_ms",
        ),
        adapter_prepare_ms: nullableDecimal(
          leg.adapter_prepare_ms ?? null,
          "latency trace leg.adapter_prepare_ms",
        ),
        journal_append_ms: nullableDecimal(
          leg.journal_append_ms ?? null,
          "latency trace leg.journal_append_ms",
        ),
        guard_to_submit_ms: nullableDecimal(
          leg.guard_to_submit_ms,
          "latency trace leg.guard_to_submit_ms",
        ),
        submit_to_ack_ms: nullableDecimal(
          leg.submit_to_ack_ms,
          "latency trace leg.submit_to_ack_ms",
        ),
        ack_to_first_fill_ms: nullableDecimal(
          leg.ack_to_first_fill_ms,
          "latency trace leg.ack_to_first_fill_ms",
        ),
        ack_to_terminal_ms: nullableDecimal(
          leg.ack_to_terminal_ms,
          "latency trace leg.ack_to_terminal_ms",
        ),
      }
    }),
  }
}

function parseJournal(value: unknown): ExecutionJournal {
  const item = record(value, "execution journal")
  return {
    executionId: stringValue(item.execution_id, "journal.execution_id"),
    monitorType:
      item.monitor_type === "cycle" || item.monitor_type === "regular"
        ? item.monitor_type
        : null,
    monitorKey: nullableString(item.monitor_key, "journal.monitor_key"),
    underlying: nullableString(item.underlying, "journal.underlying"),
    intervalSeconds:
      item.interval_seconds == null
        ? null
        : decimal(item.interval_seconds, "journal.interval_seconds"),
    status: stringValue(item.status, "journal.status"),
    leg1: parseLeg(item.leg1),
    leg2: parseLeg(item.leg2),
    residualQuantity: decimal(
      item.residual_quantity,
      "journal.residual_quantity",
    ),
    grossLockedPnlUsd: nullableDecimal(
      item.gross_locked_pnl_usd ?? null,
      "journal.gross_locked_pnl_usd",
    ),
    totalFeeSettlementCostUsd: nullableDecimal(
      item.total_fee_settlement_cost_usd ?? null,
      "journal.total_fee_settlement_cost_usd",
    ),
    netLockedPnlUsd: nullableDecimal(
      item.net_locked_pnl_usd ?? null,
      "journal.net_locked_pnl_usd",
    ),
    manualResolution:
      item.manual_resolution == null
        ? null
        : parseManualResolution(item.manual_resolution),
    latencyTrace:
      item.latency_trace == null
        ? null
        : parseLatencyTrace(item.latency_trace),
    portfolioId: nullableString(
      item.portfolio_id,
      "journal.portfolio_id",
    ),
    strategyId: nullableString(item.strategy_id, "journal.strategy_id"),
    lastError: nullableString(item.last_error, "journal.last_error"),
    createdAt: timestamp(item.created_at, "journal.created_at"),
    updatedAt: timestamp(item.updated_at, "journal.updated_at"),
  }
}

function parseRecovery(value: unknown): ExposureRecovery {
  const item = record(value, "exposure recovery")
  const attempts = decimal(item.attempts, "recovery.attempts")
  if (!Number.isInteger(attempts) || attempts < 0) {
    throw new Error("recovery.attempts must be a non-negative integer")
  }
  return {
    recoveryId: stringValue(item.recovery_id, "recovery.recovery_id"),
    executionId: nullableString(item.execution_id, "recovery.execution_id"),
    route: nullableString(item.route, "recovery.route"),
    venueId: stringValue(item.venue_id, "recovery.venue_id"),
    contractId: stringValue(item.contract_id, "recovery.contract_id"),
    side: stringValue(item.side, "recovery.side"),
    quantity: decimal(item.quantity, "recovery.quantity"),
    filledQuantity: decimal(
      item.filled_quantity,
      "recovery.filled_quantity",
    ),
    limitPrice: decimal(item.limit_price, "recovery.limit_price"),
    averagePrice: nullableDecimal(item.average_price, "recovery.average_price"),
    sourceContractId: nullableString(
      item.source_contract_id,
      "recovery.source_contract_id",
    ),
    sourceSide: nullableString(item.source_side, "recovery.source_side"),
    sourcePrice: nullableDecimal(item.source_price, "recovery.source_price"),
    sourceFeeAmount: nullableDecimal(
      item.source_fee_amount,
      "recovery.source_fee_amount",
    ),
    sourceFeeCurrency: nullableString(
      item.source_fee_currency,
      "recovery.source_fee_currency",
    ),
    estimatedVwap: nullableDecimal(
      item.estimated_vwap,
      "recovery.estimated_vwap",
    ),
    estimatedRecoveryFeeAmount: nullableDecimal(
      item.estimated_recovery_fee_amount,
      "recovery.estimated_recovery_fee_amount",
    ),
    estimatedRecoveryFeeCurrency: nullableString(
      item.estimated_recovery_fee_currency,
      "recovery.estimated_recovery_fee_currency",
    ),
    estimatedGrossResult: nullableDecimal(
      item.estimated_gross_result,
      "recovery.estimated_gross_result",
    ),
    estimatedNetResult: nullableDecimal(
      item.estimated_net_result,
      "recovery.estimated_net_result",
    ),
    recoveryFeeAmount: nullableDecimal(
      item.recovery_fee_amount,
      "recovery.recovery_fee_amount",
    ),
    recoveryFeeCurrency: nullableString(
      item.recovery_fee_currency,
      "recovery.recovery_fee_currency",
    ),
    actualGrossResult: nullableDecimal(
      item.actual_gross_result,
      "recovery.actual_gross_result",
    ),
    actualNetResult: nullableDecimal(
      item.actual_net_result,
      "recovery.actual_net_result",
    ),
    portfolioId: nullableString(
      item.portfolio_id,
      "recovery.portfolio_id",
    ),
    strategyId: nullableString(item.strategy_id, "recovery.strategy_id"),
    status: stringValue(item.status, "recovery.status"),
    attempts,
    clientOrderId: nullableString(
      item.client_order_id,
      "recovery.client_order_id",
    ),
    orderId: nullableString(item.order_id, "recovery.order_id"),
    lastError: nullableString(item.last_error, "recovery.last_error"),
    createdAt: timestamp(item.created_at, "recovery.created_at"),
    updatedAt: timestamp(item.updated_at, "recovery.updated_at"),
  }
}

/**
 * Validate and normalize the persisted execution snapshot emitted by FastAPI.
 *
 * @throws When a required field has an invalid type or value.
 */
export function parseExecutionActivity(
  value: unknown,
): ExecutionActivitySnapshot {
  const message = record(value, "execution activity")
  if (message.type !== "execution_activity_snapshot") {
    throw new Error("message is not an execution activity snapshot")
  }
  return {
    type: message.type,
    generatedAt: timestamp(message.generated_at, "generated_at"),
    tradingFeesUsd: decimal(message.trading_fees_usd, "trading_fees_usd"),
    gasUsd: decimal(message.gas_usd, "gas_usd"),
    orders: list(message.orders, "orders", parseOrder),
    trades: list(message.trades, "trades", parseTrade),
    positions: list(message.positions, "positions", parsePosition),
    journals: list(message.journals, "journals", parseJournal),
    recoveries: list(message.recoveries, "recoveries", parseRecovery),
  }
}
