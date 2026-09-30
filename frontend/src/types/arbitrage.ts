export type Underlying = string
export type IntervalSeconds = number
export type SignalDirection = "buy" | "sell"
export type OpportunitySide = "LONG" | "SHORT" | "MIXED"
export type MonitorType = "cycle" | "regular"
export type ConnectionStatus =
  | "connecting"
  | "connected"
  | "disconnected"
  | "error"

export interface ArbitrageSignal {
  contractId: string
  venueId: string
  direction: SignalDirection
  quantity: number
  limitPrice: number
  fairProbability: number
  edge: number
  strategyId: string
  generatedAt: string
}

/** A validated two-leg opportunity received from the backend signal stream. */
export interface ArbitrageOpportunity {
  id: string
  monitorType: MonitorType
  monitorKey: string
  marketLabel: string
  underlying: Underlying | null
  intervalSeconds: IntervalSeconds | null
  side: OpportunitySide
  signals: [ArbitrageSignal, ArbitrageSignal]
  generatedAt: string
}

type UnknownRecord = Record<string, unknown>

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null
}

function parseDecimal(value: unknown, field: string): number {
  if (typeof value !== "string" && typeof value !== "number") {
    throw new Error(`${field} must be a decimal`)
  }
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) {
    throw new Error(`${field} must be finite`)
  }
  return parsed
}

function parseSignal(value: unknown): ArbitrageSignal {
  if (!isRecord(value)) {
    throw new Error("signal must be an object")
  }
  const direction = value.direction
  if (direction !== "buy" && direction !== "sell") {
    throw new Error("signal direction is invalid")
  }
  const generatedAt = String(value.generated_at ?? "")
  if (Number.isNaN(Date.parse(generatedAt))) {
    throw new Error("signal timestamp is invalid")
  }
  return {
    contractId: String(value.contract_id ?? ""),
    venueId: String(value.venue_id ?? ""),
    direction,
    quantity: parseDecimal(value.quantity, "quantity"),
    limitPrice: parseDecimal(value.limit_price, "limit_price"),
    fairProbability: parseDecimal(
      value.fair_probability,
      "fair_probability",
    ),
    edge: parseDecimal(value.edge, "edge"),
    strategyId: String(value.strategy_id ?? ""),
    generatedAt,
  }
}

/**
 * Validate an unknown WebSocket payload and normalize its snake-case fields.
 *
 * @throws When the payload is not a supported two-leg signal pair.
 */
export function parseArbitrageOpportunity(
  value: unknown,
): ArbitrageOpportunity {
  if (!isRecord(value) || value.type !== "arbitrage_signal_pair") {
    throw new Error("message is not an arbitrage signal pair")
  }
  const monitorType = value.monitor_type ?? "cycle"
  if (monitorType !== "cycle" && monitorType !== "regular") {
    throw new Error("monitor type is invalid")
  }
  const underlying =
    typeof value.underlying === "string" && value.underlying.trim()
      ? value.underlying.trim().toUpperCase()
      : null
  const intervalSeconds =
    typeof value.interval_seconds === "number" &&
    Number.isInteger(value.interval_seconds) &&
    value.interval_seconds > 0
      ? value.interval_seconds
      : null
  if (monitorType === "cycle" && (!underlying || !intervalSeconds)) {
    throw new Error("cycle identity is invalid")
  }
  const monitorKey = String(
    value.monitor_key ?? `cycle:${underlying}:${intervalSeconds}`,
  )
  const marketLabel = String(value.market_label ?? underlying ?? "Regular")
  if (!monitorKey || !marketLabel) {
    throw new Error("monitor identity is invalid")
  }
  if (!Array.isArray(value.signals) || value.signals.length !== 2) {
    throw new Error("an opportunity must contain exactly two legs")
  }
  const left = parseSignal(value.signals[0])
  const right = parseSignal(value.signals[1])
  const side =
    left.direction === "buy" && right.direction === "buy"
      ? "LONG"
      : left.direction === "sell" && right.direction === "sell"
        ? "SHORT"
        : "MIXED"
  const generatedAt =
    Date.parse(left.generatedAt) >= Date.parse(right.generatedAt)
      ? left.generatedAt
      : right.generatedAt
  const signalIds = [left, right]
    .map(
      (signal) =>
        `${signal.strategyId}:${signal.contractId}:${signal.direction}`,
    )
    .sort()
  return {
    id: [monitorKey, generatedAt, ...signalIds].join(":"),
    monitorType,
    monitorKey,
    marketLabel,
    underlying,
    intervalSeconds,
    side,
    signals: [left, right],
    generatedAt,
  }
}
