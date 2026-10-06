import {
  Activity,
  ArrowRight,
  Database,
  EyeOff,
  LoaderCircle,
  RadioTower,
  TriangleAlert,
} from "lucide-react"
import { useState } from "react"

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
import { formatTimestamp } from "@/lib/formatters"

/** Present selected policy events and their cross-venue observations. */
export function DashboardPage() {
  const backend = useBackendStatus()
  const runtime = useRuntime()
  const trading = useTradingRun()
  const stream = useArbitrageStream()
  const [stoppingMonitor, setStoppingMonitor] = useState<string | null>(null)
  const [shortMarketKeys, setShortMarketKeys] = useState<string[]>([])
  const regularMarkets = runtime.runtime?.regular_markets ?? []
  const tradingActive = ["preparing", "running", "stopping"].includes(
    trading.run?.status ?? "",
  )
  const selectedShortMarketKeys = tradingActive
    ? (trading.run?.short_market_keys ?? shortMarketKeys)
    : shortMarketKeys
  const shortSelectionLocked =
    tradingActive || trading.requestState === "loading"
  const opportunities = stream.opportunities.filter(
    (opportunity) => opportunity.monitorType === "regular",
  )

  async function unmonitor(monitorKey: string) {
    setStoppingMonitor(monitorKey)
    try {
      await runtime.unmonitor(monitorKey)
      setShortMarketKeys((current) =>
        current.filter((key) => key !== monitorKey),
      )
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
                        title="Prepare covered short collateral when trading starts"
                        disabled={
                          shortSelectionLocked ||
                          (market.pair_count === 0 &&
                            !selectedShortMarketKeys.includes(
                              market.monitor_key,
                            ))
                        }
                        onClick={() => toggleShortMarket(market.monitor_key)}
                      >
                        {selectedShortMarketKeys.includes(market.monitor_key)
                          ? "Short enabled"
                          : "Enable short"}
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
                Two-leg opportunities from explicitly selected policy markets.
              </CardDescription>
            </div>
            <Badge variant="outline">{opportunities.length} observed</Badge>
          </CardHeader>
          <CardContent className="px-2 sm:px-5">
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
              <span className="text-xs text-muted-foreground">Messages</span>
              <span className="numeric text-xs font-semibold">
                {stream.signalCount}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-xs text-muted-foreground">Last update</span>
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
