import {
  Activity,
  ArrowRight,
  Database,
  EyeOff,
  LoaderCircle,
  RadioTower,
  TriangleAlert,
} from "lucide-react"
import { useEffect, useRef, useState } from "react"

import { StatusCard } from "@/components/dashboard/status-card"
import { PageHeader } from "@/components/layout/page-header"
import { TradingControl } from "@/components/runtime/trading-control"
import { OpportunityTable } from "@/components/signals/opportunity-table"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { useArbitrageStream } from "@/features/arbitrage/arbitrage-stream-provider"
import { useRuntime } from "@/features/runtime/use-runtime"
import { useBackendStatus } from "@/features/system/use-backend-status"
import { useTradingRun } from "@/features/trading/use-trading-run"
import { readTradingPreferences, saveTradingPreferences } from "@/features/trading/trading-preferences"
import { formatTimestamp } from "@/lib/formatters"
import type { MarketMatchPair } from "@/types/api"
import type { InventorySellPair } from "@/types/trading"

function inventoryPairKey(pair: MarketMatchPair): InventorySellPair {
  return [pair.left.id, pair.right.id].sort() as InventorySellPair
}

function selectedInventoryPair(pair: MarketMatchPair, selected: InventorySellPair[]) {
  const [left, right] = inventoryPairKey(pair)
  return selected.some((key) => key.includes(left) && key.includes(right))
}

function sellPairLabel(pair: MarketMatchPair) {
  return [pair.left, pair.right].map((contract) => {
    const outcome = contract.outcome_id.match(/(?:^|:)(yes|no)(?=:|$)/i)?.[1]
    return `${outcome?.toUpperCase() ?? "Outcome"} ${contract.venue_id}`
  }).join(" + ")
}

/** Present selected policy events and their cross-venue observations. */
export function DashboardPage() {
  const backend = useBackendStatus()
  const runtime = useRuntime()
  const trading = useTradingRun()
  const stream = useArbitrageStream()
  const [stoppingMonitor, setStoppingMonitor] = useState<string | null>(null)
  const [savedSettings] = useState(readTradingPreferences)
  const [shortMarketKeys, setShortMarketKeys] = useState<string[]>(savedSettings.short_market_keys ?? [])
  const [inventoryPairs, setInventoryPairs] = useState<InventorySellPair[]>(savedSettings.existing_inventory_pairs ?? [])
  const restoredRunId = useRef<string | null>(null)
  const regularMarkets = runtime.runtime?.regular_markets ?? []
  const tradingActive = ["preparing", "running", "stopping"].includes(
    trading.run?.status ?? "",
  )
  const selectedShortMarketKeys = (tradingActive
    ? (trading.run?.short_market_keys ?? shortMarketKeys)
    : shortMarketKeys).filter((key) => regularMarkets.some((market) => market.monitor_key === key))
  const selectedInventoryPairs = (tradingActive
    ? (trading.run?.existing_inventory_pairs ?? [])
    : inventoryPairs).filter((key) => regularMarkets.some((market) =>
      market.pairs.some((pair) => selectedInventoryPair(pair, [key])),
    ))
  const manualInventoryMarketKeys = regularMarkets.filter((market) =>
    selectedShortMarketKeys.includes(market.monitor_key)
    || market.pairs.some((pair) => selectedInventoryPair(pair, selectedInventoryPairs)),
  ).map((market) => market.monitor_key)
  const shortSelectionLocked =
    tradingActive || trading.requestState === "loading"
  const opportunities = stream.opportunities.filter(
    (opportunity) => opportunity.monitorType === "regular",
  )

  useEffect(() => {
    const run = trading.run
    if (!run || restoredRunId.current === run.id) return
    restoredRunId.current = run.id
    if (!tradingActive && savedSettings.short_market_keys !== undefined) return
    setShortMarketKeys(run.short_market_keys)
    setInventoryPairs(run.existing_inventory_pairs ?? [])
  }, [trading.run, tradingActive, savedSettings])

  useEffect(() => {
    saveTradingPreferences({
      short_market_keys: shortMarketKeys,
      existing_inventory_pairs: inventoryPairs,
    })
  }, [shortMarketKeys, inventoryPairs])

  async function unmonitor(monitorKey: string) {
    setStoppingMonitor(monitorKey)
    try {
      await runtime.unmonitor(monitorKey)
      setShortMarketKeys((current) =>
        current.filter((key) => key !== monitorKey),
      )
      const removed = regularMarkets.find((market) => market.monitor_key === monitorKey)
      setInventoryPairs((current) => current.filter((key) =>
        !removed?.pairs.some((pair) => selectedInventoryPair(pair, [key])),
      ))
    } finally {
      setStoppingMonitor(null)
    }
  }

  function toggleShortMarket(monitorKey: string) {
    if (shortSelectionLocked) return
    setShortMarketKeys((current) =>
      current.includes(monitorKey)
        ? current.filter((key) => key !== monitorKey)
        : [...current, monitorKey],
    )
  }

  function toggleInventoryPair(pair: MarketMatchPair) {
    if (shortSelectionLocked) return
    setInventoryPairs((current) => selectedInventoryPair(pair, current)
      ? current.filter((key) => !selectedInventoryPair(pair, [key]))
      : [...current, inventoryPairKey(pair)],
    )
  }

  function toggleAllInventoryPairs(pairs: MarketMatchPair[]) {
    if (shortSelectionLocked) return
    setInventoryPairs((current) => {
      const otherPairs = current.filter((key) =>
        !pairs.some((pair) => selectedInventoryPair(pair, [key])),
      )
      return pairs.every((pair) => selectedInventoryPair(pair, current))
        ? otherPairs
        : [...otherPairs, ...pairs.map(inventoryPairKey)]
    })
  }

  const connectionTone =
    stream.connectionStatus === "connected"
      ? "positive"
      : stream.connectionStatus === "error"
        ? "negative"
        : "warning"

  return (
    <>
      <PageHeader
        eyebrow="Event intelligence"
        title="Policy arbitrage monitor"
        description="Observe only the multi-venue policy markets selected from the event catalog."
        actions={
          <TradingControl
            controller={trading}
            signalSettings={runtime.runtime?.signal_settings}
            shortMarketKeys={selectedShortMarketKeys}
            existingInventoryPairs={selectedInventoryPairs}
            manualInventoryMarketKeys={manualInventoryMarketKeys}
          />
        }
      />

      <aside
        aria-label="Real-money trading notice"
        className="mb-5 flex items-start gap-3 rounded-xl border border-amber-500/25 bg-amber-500/5 p-4"
      >
        <TriangleAlert
          aria-hidden="true"
          className="mt-0.5 size-5 shrink-0 text-amber-700 dark:text-amber-300"
        />
        <div>
          <p className="text-sm font-medium">
            {tradingActive
              ? "Live trading can use real funds"
              : "Explore the market before enabling real-money trading"}
          </p>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            Monitoring events and inspecting feed latency do not require live
            trading. Enabling trading can submit real orders; signals do not
            guarantee profit, and unmatched fills can lose money. Review the
            warnings and limits before starting.
          </p>
        </div>
      </aside>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <StatusCard
          label="Backend API"
          value={
            backend.loading && backend.health === null
              ? "Checking"
              : backend.health
                ? "Online"
                : "Offline"
          }
          detail={backend.error ?? "Control API is responding"}
          icon={Activity}
          tone={backend.health ? "positive" : "negative"}
        />
        <StatusCard
          label="API readiness"
          value={backend.ready ? "Ready" : "Not ready"}
          detail="Application readiness endpoint"
          icon={Database}
          tone={backend.ready ? "positive" : "warning"}
        />
        <StatusCard
          label="Selected events"
          value={String(regularMarkets.length)}
          detail="Explicit multi-venue monitors"
          icon={RadioTower}
          tone={regularMarkets.length > 0 ? "positive" : "warning"}
        />
        <StatusCard
          label="Event WebSockets"
          value={stream.connectionStatus}
          detail="Selected regular markets only"
          icon={RadioTower}
          tone={connectionTone}
        />
      </section>

      <section className="mt-4">
        <Card>
          <CardHeader>
            <CardTitle>Connected events</CardTitle>
            <CardDescription>
              Native order books currently included in cross-venue detection.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-3 lg:grid-cols-2">
            {regularMarkets.length === 0 ? (
              <div className="rounded-xl border border-dashed border-border bg-background/40 p-6 sm:p-8 lg:col-span-2">
                <div className="flex flex-col justify-between gap-5 sm:flex-row sm:items-center">
                  <div>
                    <RadioTower className="mb-4 size-7 text-primary" />
                    <h3 className="text-lg font-semibold tracking-tight">
                      Start with an event you want to follow
                    </h3>
                    <p className="mt-2 max-w-xl text-sm leading-relaxed text-muted-foreground">
                      Connect the same policy question across two or more
                      venues. Its order books and opportunities will appear
                      here.
                    </p>
                  </div>
                  <Button asChild>
                    <a href="/events">
                      Explore event catalog <ArrowRight className="size-4" />
                    </a>
                  </Button>
                </div>
                <ol className="mt-7 grid gap-4 border-t border-border pt-5 sm:grid-cols-3">
                  {[
                    [
                      "Find an event",
                      "Search policy decisions and political markets.",
                    ],
                    [
                      "Connect venues",
                      "Select a group with at least two supported venues.",
                    ],
                    [
                      "Follow the flow",
                      "See live books, opportunities and pipeline activity.",
                    ],
                  ].map(([title, detail], index) => (
                    <li key={title} className="flex gap-3">
                      <span className="flex size-6 shrink-0 items-center justify-center rounded-full bg-primary/10 text-xs font-semibold text-primary">
                        {index + 1}
                      </span>
                      <div>
                        <p className="text-xs font-medium">{title}</p>
                        <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                          {detail}
                        </p>
                      </div>
                    </li>
                  ))}
                </ol>
              </div>
            ) : (
              regularMarkets.map((market) => (
                <div
                  key={market.monitor_key}
                  className="rounded-lg border border-border/70 bg-background/35 p-3"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="line-clamp-2 break-words text-xs font-medium">
                        {market.markets[0]?.title ?? "Policy event"}
                      </p>
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        {market.markets.map((venueMarket) => (
                          <Badge key={venueMarket.venue_id} variant="outline">
                            {venueMarket.venue_id}
                          </Badge>
                        ))}
                        <Badge variant="secondary">
                          {market.pair_count} pair
                          {market.pair_count === 1 ? "" : "s"}
                        </Badge>
                      </div>
                    </div>
                    <div className="flex shrink-0 items-center gap-1">
                      <Button
                        type="button"
                        size="sm"
                        variant={
                          selectedShortMarketKeys.includes(market.monitor_key)
                            ? "default"
                            : "outline"
                        }
                        aria-label={`${selectedShortMarketKeys.includes(market.monitor_key) ? "Disable" : "Enable"} short arbitrage for ${market.markets[0]?.title ?? "Policy event"}`}
                        aria-pressed={selectedShortMarketKeys.includes(
                          market.monitor_key,
                        )}
                        title="Prepare collateral with splits for this event. Disable existing-inventory pairs first."
                        disabled={
                          shortSelectionLocked ||
                          market.pairs.some((pair) => selectedInventoryPair(pair, selectedInventoryPairs)) ||
                          (market.pair_count === 0 &&
                            !selectedShortMarketKeys.includes(
                              market.monitor_key,
                            ))
                        }
                        onClick={() => toggleShortMarket(market.monitor_key)}
                      >
                        {selectedShortMarketKeys.includes(market.monitor_key)
                          ? "Split mode enabled"
                          : "Prepare collateral"}
                      </Button>
                      <Button
                        aria-label="Disconnect policy event"
                        title="Disconnect"
                        variant="ghost"
                        size="icon"
                        disabled={stoppingMonitor !== null}
                        onClick={() => void unmonitor(market.monitor_key)}
                      >
                        {stoppingMonitor === market.monitor_key ? (
                          <LoaderCircle className="size-4 animate-spin" />
                        ) : (
                          <EyeOff className="size-4" />
                        )}
                      </Button>
                    </div>
                  </div>
                  {market.pairs.length > 0 && (
                    <details className="mt-3 border-t border-border/60 pt-3" open={market.pairs.some((pair) => selectedInventoryPair(pair, selectedInventoryPairs)) || undefined}>
                      <summary className="cursor-pointer text-xs font-medium">
                        Sell existing inventory
                      </summary>
                      <p className="mt-2 text-xs text-muted-foreground">
                        Choose the exact tokens to sell together. Uses available
                        accounted tokens only, with no splits or automatic top-ups.
                        Balances are verified when trading starts.
                      </p>
                      <fieldset
                        aria-label={`Existing inventory pairs for ${market.markets[0]?.title ?? "Policy event"}`}
                        disabled={shortSelectionLocked || selectedShortMarketKeys.includes(market.monitor_key)}
                        className="mt-2 grid gap-2 disabled:opacity-60"
                      >
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          className="justify-self-start"
                          onClick={() => toggleAllInventoryPairs(market.pairs)}
                        >
                          {market.pairs.every((pair) => selectedInventoryPair(pair, selectedInventoryPairs))
                            ? "Clear all" : "Select all"}
                        </Button>
                        {market.pairs.map((pair) => (
                          <label key={JSON.stringify(inventoryPairKey(pair))} className="flex items-start gap-2 text-xs">
                            <input
                              type="checkbox"
                              className="mt-0.5 size-4 shrink-0 accent-primary"
                              checked={selectedInventoryPair(pair, selectedInventoryPairs)}
                              onChange={() => toggleInventoryPair(pair)}
                            />
                            {sellPairLabel(pair)}
                          </label>
                        ))}
                      </fieldset>
                      {selectedShortMarketKeys.includes(market.monitor_key) && (
                        <p className="mt-2 text-xs text-muted-foreground">
                          Disable split mode to select existing inventory for this event.
                        </p>
                      )}
                    </details>
                  )}
                </div>
              ))
            )}
            {runtime.error && (
              <p className="text-xs text-rose-300 lg:col-span-2">
                {runtime.error}
              </p>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="mt-4 grid gap-4 xl:grid-cols-12">
        <Card className="xl:col-span-9">
          <CardHeader className="flex-row items-start justify-between">
            <div>
              <CardTitle>Event opportunity history</CardTitle>
              <CardDescription>
                Saved detections and live signals from selected events. Past signals may no longer be tradable.
              </CardDescription>
            </div>
            <Badge variant="outline">{opportunities.length} observed</Badge>
          </CardHeader>
          <CardContent className="px-2 sm:px-5">
            {stream.historyError && <p role="alert" className="mb-3 text-xs text-amber-600 dark:text-amber-300">{stream.historyError}</p>}
            <OpportunityTable opportunities={opportunities} />
          </CardContent>
        </Card>

        <Card className="xl:col-span-3">
          <CardHeader>
            <CardTitle>Feed activity</CardTitle>
            <CardDescription>Only selected event streams.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between border-b border-border/60 pb-3">
              <span className="text-xs text-muted-foreground">Live messages this session</span>
              <span className="numeric text-xs font-semibold">
                {stream.signalCount}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-xs text-muted-foreground">Last live message</span>
              <span className="numeric text-xs font-medium">
                {formatTimestamp(stream.lastReceivedAt)}
              </span>
            </div>
            {stream.connectionError && (
              <p className="rounded-lg border border-rose-400/20 bg-rose-400/8 p-3 text-xs text-rose-300">
                {stream.connectionError}
              </p>
            )}
          </CardContent>
        </Card>
      </section>
    </>
  )
}
