import { useEffect, useMemo, useState } from "react"

import { PageHeader } from "@/components/layout/page-header"
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { useArbitrageStream } from "@/features/arbitrage/arbitrage-stream-provider"
import {
  filterOpportunities,
  type ArbitrageFilters,
} from "@/features/arbitrage/filters"
import { useMarketMatches } from "@/features/markets/use-market-matches"
import { useRuntime } from "@/features/runtime/use-runtime"
import { intervalLabel } from "@/lib/formatters"

/** Present live cycle and regular-market arbitrage signals with filters. */
export function SignalsPage() {
  const stream = useArbitrageStream()
  const marketMatches = useMarketMatches()
  const runtime = useRuntime()
  const [minNetEdge, setMinNetEdge] = useState("0")
  const [costBuffer, setCostBuffer] = useState("0")
  const [filters, setFilters] = useState<ArbitrageFilters>({
    underlying: "ALL",
    interval: "ALL",
    side: "ALL",
  })
  const opportunities = useMemo(
    () => filterOpportunities(stream.opportunities, filters),
    [stream.opportunities, filters],
  )
  const assets = [
    ...new Set([
      ...marketMatches.matches.map((market) => market.underlying),
      ...stream.opportunities.flatMap((opportunity) =>
        opportunity.underlying ? [opportunity.underlying] : [],
      ),
    ]),
  ].sort()
  const intervals = [
    ...new Set([
      ...marketMatches.matches.map((market) => market.interval_seconds),
      ...stream.opportunities.flatMap((opportunity) =>
        opportunity.intervalSeconds ? [opportunity.intervalSeconds] : [],
      ),
    ]),
  ].sort((left, right) => left - right)
  const currentMinEdge = runtime.runtime?.signal_settings?.min_net_edge
  const currentCostBuffer = runtime.runtime?.signal_settings?.cost_buffer

  useEffect(() => {
    if (currentMinEdge !== undefined) setMinNetEdge(String(currentMinEdge))
    if (currentCostBuffer !== undefined) {
      setCostBuffer(String(currentCostBuffer))
    }
  }, [currentMinEdge, currentCostBuffer])

  const settings = {
    min_net_edge: Number(minNetEdge),
    cost_buffer: Number(costBuffer),
  }
  const settingsValid =
    settings.min_net_edge >= 0 &&
    settings.min_net_edge <= 0.2 &&
    settings.cost_buffer >= 0 &&
    settings.cost_buffer <= 0.05

  return (
    <>
      <PageHeader
        eyebrow="Signal tape"
        title="Arbitrage opportunities"
        description="A bounded feed of the latest two-leg opportunities received from FastAPI."
      />

      <Card>
        <CardHeader className="gap-4 xl:flex-row xl:items-center xl:justify-between">
          <div>
            <div className="flex items-center gap-2">
              <CardTitle>Live signal feed</CardTitle>
              <Badge
                variant={
                  stream.connectionStatus === "connected"
                    ? "positive"
                    : "warning"
                }
              >
                {stream.connectionStatus}
              </Badge>
            </div>
            <CardDescription>
              Stored in memory up to the latest 200 opportunities.
            </CardDescription>
          </div>

          <div className="flex flex-wrap gap-2">
            <Select
              value={filters.underlying}
              onValueChange={(value) =>
                setFilters((current) => ({
                  ...current,
                  underlying: value as ArbitrageFilters["underlying"],
                }))
              }
            >
              <SelectTrigger aria-label="Filter by underlying">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">All assets</SelectItem>
                {assets.map((asset) => (
                  <SelectItem key={asset} value={asset}>
                    {asset}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select
              value={String(filters.interval)}
              onValueChange={(value) =>
                setFilters((current) => ({
                  ...current,
                  interval:
                    value === "ALL"
                      ? "ALL"
                      : Number(value),
                }))
              }
            >
              <SelectTrigger aria-label="Filter by interval">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">All cycles</SelectItem>
                {intervals.map((value) => (
                  <SelectItem key={value} value={String(value)}>
                    {intervalLabel(value)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select
              value={filters.side}
              onValueChange={(value) =>
                setFilters((current) => ({
                  ...current,
                  side: value as ArbitrageFilters["side"],
                }))
              }
            >
              <SelectTrigger aria-label="Filter by opportunity type">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">Long & short</SelectItem>
                <SelectItem value="LONG">Long</SelectItem>
                <SelectItem value="SHORT">Short</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <div className="mb-4 flex flex-wrap items-end gap-3 rounded-lg border border-border/70 bg-background/35 p-3">
            <label className="block min-w-48 flex-1 text-xs font-medium">
              Minimum net edge (USD/contract)
              <input
                type="number"
                min="0"
                max="0.2"
                step="0.001"
                value={minNetEdge}
                onChange={(event) => setMinNetEdge(event.target.value)}
                className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </label>
            <label className="block min-w-48 flex-1 text-xs font-medium">
              Cost buffer (USD/contract)
              <input
                type="number"
                min="0"
                max="0.05"
                step="0.001"
                value={costBuffer}
                onChange={(event) => setCostBuffer(event.target.value)}
                className="mt-2 h-9 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </label>
            <Button
              variant="secondary"
              disabled={
                !settingsValid ||
                runtime.requestState === "loading" ||
                runtime.runtime?.trading_enabled
              }
              onClick={() => void runtime.updateSignalSettings(settings)}
            >
              Apply signal settings
            </Button>
            <p className="w-full text-[11px] text-muted-foreground">
              Fees are always deducted. These values add the required profit
              and safety margin before a signal is emitted.
            </p>
            {runtime.runtime?.trading_enabled && (
              <p className="w-full text-[11px] text-amber-300">
                Disable live trading before changing signal settings.
              </p>
            )}
            {runtime.error && (
              <p className="w-full text-[11px] text-rose-300">
                {runtime.error}
              </p>
            )}
          </div>
          <OpportunityTable opportunities={opportunities} />
        </CardContent>
      </Card>
    </>
  )
}
