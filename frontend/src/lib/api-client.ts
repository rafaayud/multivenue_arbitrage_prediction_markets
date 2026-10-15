import { API_BASE_URL, MAX_RECENT_OPPORTUNITIES } from "@/lib/config"
import { parseStoredArbitrageOpportunity } from "@/types/arbitrage"
import type {
  MessageResponse,
} from "@/types/api"
import type {
  ArbitrageCandidate,
  RegularMarketMonitor,
} from "@/types/agg"
import type { RuntimeState, SignalSettings } from "@/types/runtime"
import type { PnlDashboard, PnlView, TimeRange } from "@/types/pnl"
import type { VenueHealthReport } from "@/types/venue-health"
import type { ManualExecutionResolutionInput } from "@/types/execution"
import type {
  TradingRun,
  TradingRunSettings,
  TradingSession,
} from "@/types/trading"

/** Preserve an unsuccessful backend response's readable message and HTTP status. */
export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message)
  }
}

async function errorMessage(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { detail?: unknown }
    if (body.detail && typeof body.detail === "object") {
      const detail = body.detail as {
        message?: unknown
        venue_health_issues?: unknown
      }
      if (typeof detail.message === "string") {
        const issues = Array.isArray(detail.venue_health_issues)
          ? detail.venue_health_issues.filter((item): item is string =>
              typeof item === "string")
          : []
        return [detail.message, ...issues].join(": ")
      }
    }
    return typeof body.detail === "string"
      ? body.detail
      : JSON.stringify(body.detail ?? body)
  } catch {
    return response.statusText || `HTTP ${response.status}`
  }
}

async function request<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    credentials: "include",
    headers: {
      Accept: "application/json",
      ...init?.headers,
    },
  })
  if (!response.ok) {
    throw new ApiError(await errorMessage(response), response.status)
  }
  return (await response.json()) as T
}

/** Typed HTTP operations used by dashboard features. */
export const apiClient = {
  health: (signal?: AbortSignal) =>
    request<MessageResponse>("/health", { signal }),

  ready: (signal?: AbortSignal) =>
    request<MessageResponse>("/ready", { signal }),

  venueHealth: (signal?: AbortSignal) =>
    request<VenueHealthReport>("/venue-health", { signal }),

  runtimeStatus: (signal?: AbortSignal) =>
    request<RuntimeState>("/runtime/status", { signal }),

  arbitrageOpportunities: async (signal?: AbortSignal) => {
    const rows = await request<unknown[]>(
      `/arbitrage-opportunities?limit=${MAX_RECENT_OPPORTUNITIES}`,
      { signal },
    )
    return rows.map(parseStoredArbitrageOpportunity)
  },

  updateSignalSettings: (settings: SignalSettings) =>
    request<RuntimeState>("/runtime/signal-settings", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(settings),
    }),

  currentTradingRun: (signal?: AbortSignal) =>
    request<TradingRun | null>("/trading-runs/current", { signal }),

  tradingSession: (signal?: AbortSignal) =>
    request<TradingSession>("/trading-runs/session", { signal }),

  createTradingSession: (tradingKey: string) =>
    request<TradingSession>("/trading-runs/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ trading_key: tradingKey }),
    }),

  startTrading: (settings: TradingRunSettings) =>
    request<TradingRun>("/trading-runs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        live: true,
        confirmation: "LIVE",
        ...settings,
      }),
    }),

  /** Adopt explicitly declared fee-free split inventory without placing orders. */
  importManualSplits: (tradingKey: string, marketKeys: string[]) =>
    request<{ venue_id: string; market_id: string; quantity: string }[]>(
      "/runtime/inventory/manual-splits",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Trading-Key": tradingKey,
        },
        body: JSON.stringify({
          market_keys: marketKeys,
          confirm_fee_free_splits: true,
        }),
      },
    ),

  stopTrading: (runId: string) =>
    request<TradingRun>(`/trading-runs/${runId}/stop`, {
      method: "POST",
    }),

  arbitrageCandidates: (options: {
    limit?: number
    minReturn?: number
    topics?: readonly string[]
    searchText?: string
    liveOnly?: boolean
    signal?: AbortSignal
  } = {}) => {
    const query = new URLSearchParams()
    query.set("limit", String(options.limit ?? 100))
    if (options.minReturn !== undefined) {
      query.set("min_return", String(options.minReturn))
    }
    options.topics?.forEach((topic) => query.append("topics", topic))
    if (options.searchText) {
      query.set("search_text", options.searchText)
    }
    if (options.liveOnly) {
      query.set("live_only", "true")
    }
    return request<ArbitrageCandidate[]>(
      `/arbitrage-candidates?${query.toString()}`,
      { signal: options.signal },
    )
  },

  marketCatalog: (options: {
    limit?: number
    topics?: readonly string[]
    searchText?: string
    liveOnly?: boolean
    signal?: AbortSignal
  } = {}) => {
    const query = new URLSearchParams()
    query.set("limit", String(options.limit ?? 500))
    options.topics?.forEach((topic) => query.append("topics", topic))
    if (options.searchText) {
      query.set("search_text", options.searchText)
    }
    if (options.liveOnly) {
      query.set("live_only", "true")
    }
    return request<ArbitrageCandidate[]>(
      `/market-catalog?${query.toString()}`,
      { signal: options.signal },
    )
  },

  monitorArbitrageCandidate: (candidate: ArbitrageCandidate) =>
    request<RegularMarketMonitor>("/arbitrage-candidates/monitor", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        markets: candidate.markets
          .filter(({ venue_id }) =>
            ["POLYMARKET", "LIMITLESS", "PREDICT"].includes(
              venue_id.toUpperCase(),
            ),
          )
          .map(({ venue_id, external_market_id, title }) => ({
            venue_id,
            external_market_id,
            search_text: candidate.event_title ?? title,
          })),
      }),
    }),

  unmonitorRegularMarket: (monitorKey: string) => {
    const query = new URLSearchParams({ monitor_key: monitorKey })
    return request<RuntimeState>(
      `/arbitrage-candidates/monitor?${query.toString()}`,
      { method: "DELETE" },
    )
  },

  metrics: async (signal?: AbortSignal): Promise<string> => {
    const response = await fetch(`${API_BASE_URL}/metrics`, {
      signal,
      headers: { Accept: "text/plain" },
    })
    if (!response.ok) {
      throw new ApiError(await errorMessage(response), response.status)
    }
    return response.text()
  },

  pnl: (view: PnlView, range: TimeRange, signal?: AbortSignal) => {
    const query = new URLSearchParams({ view, range })
    return request<PnlDashboard>(`/pnl?${query.toString()}`, { signal })
  },

  completeExecution: (
    executionId: string,
    value: ManualExecutionResolutionInput,
  ) =>
    request<unknown>(
      `/execution-journals/${encodeURIComponent(executionId)}/complete`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          method: value.method,
          price: value.price,
          fee_amount_usd: value.feeAmountUsd,
          executed_at: value.executedAt,
          external_reference: value.externalReference,
        }),
      },
    ).then(() => undefined),
}
