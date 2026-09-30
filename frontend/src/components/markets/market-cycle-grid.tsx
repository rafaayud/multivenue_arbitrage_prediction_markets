import { Waypoints } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { intervalLabel } from "@/lib/formatters"
import type { MarketMatchesResponse } from "@/types/api"

/** Summarize monitored cycles without allocating one card per market. */
export function MarketCycleGrid({
  matches,
  loading,
  selectedShortMarketKeys,
  shortSelectionLocked,
  onShortMarketToggle,
  onAllShortMarketsToggle,
}: {
  matches: MarketMatchesResponse[]
  loading: boolean
  selectedShortMarketKeys: string[]
  shortSelectionLocked: boolean
  onShortMarketToggle: (monitorKey: string) => void
  onAllShortMarketsToggle: (monitorKeys: string[]) => void
}) {
  const ordered = [...matches].sort(
    (left, right) =>
      left.family.localeCompare(right.family) ||
      left.underlying.localeCompare(right.underlying) ||
      left.interval_seconds - right.interval_seconds,
  )
  const matched = ordered.filter((cycle) => cycle.pairs.length > 0).length
  const eligibleShortMarketKeys = ordered
    .filter((cycle) => cycle.pairs.length > 0)
    .map((cycle) => cycle.monitor_key)
  const allShortMarketsSelected =
    eligibleShortMarketKeys.length > 0 &&
    eligibleShortMarketKeys.every((key) =>
      selectedShortMarketKeys.includes(key),
    )

  return (
    <Card className="h-full">
      <CardHeader className="flex-row items-start justify-between">
        <div>
          <CardTitle>Monitored markets</CardTitle>
          <CardDescription>
            Backend-owned recurring Up/Down catalog.
          </CardDescription>
        </div>
        <div className="flex items-center gap-2">
          <Button
            type="button"
            size="sm"
            variant={allShortMarketsSelected ? "default" : "outline"}
            aria-label={
              allShortMarketsSelected
                ? "Disable short arbitrage for all matched markets"
                : "Enable short arbitrage for all matched markets"
            }
            aria-pressed={allShortMarketsSelected}
            title="Apply this selection when live trading starts"
            disabled={
              shortSelectionLocked || eligibleShortMarketKeys.length === 0
            }
            onClick={() =>
              onAllShortMarketsToggle(eligibleShortMarketKeys)
            }
          >
            {allShortMarketsSelected ? "Disable all" : "Enable all"}
          </Button>
          <Badge variant="outline">
            {matched}/{ordered.length} matched
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="px-2 sm:px-5">
        {loading && !ordered.length ? (
          <div className="space-y-3 pb-3">
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-9 w-full" />
            <Skeleton className="h-9 w-full" />
          </div>
        ) : !ordered.length ? (
          <div className="flex min-h-40 items-start gap-3 rounded-lg border border-dashed border-border p-4">
            <Waypoints className="mt-0.5 size-4 text-muted-foreground" />
            <p className="text-xs leading-relaxed text-muted-foreground">
              No recurring markets are configured by the backend.
            </p>
          </div>
        ) : (
          <div className="max-h-80 overflow-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Market</TableHead>
                  <TableHead>Cycle</TableHead>
                  <TableHead>Venues</TableHead>
                  <TableHead className="text-right">Pairs</TableHead>
                  <TableHead className="text-right">Short arbitrage</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {ordered.map((cycle) => {
                  const shortSelected = selectedShortMarketKeys.includes(
                    cycle.monitor_key,
                  )
                  const venues = [
                    ...new Set(
                      cycle.pairs.flatMap((pair) => [
                        pair.left.venue_id,
                        pair.right.venue_id,
                      ]),
                    ),
                  ]
                  return (
                    <TableRow key={cycle.monitor_key}>
                      <TableCell>
                        <span className="font-semibold">
                          {cycle.underlying}
                        </span>
                        <span className="ml-2 capitalize text-[10px] text-muted-foreground">
                          {cycle.family}
                        </span>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">
                          {intervalLabel(cycle.interval_seconds)}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs capitalize text-muted-foreground">
                        {venues.length ? venues.join(" · ") : "Waiting"}
                      </TableCell>
                      <TableCell className="numeric text-right font-semibold">
                        {cycle.pairs.length}
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          type="button"
                          size="sm"
                          variant={shortSelected ? "default" : "outline"}
                          aria-label={`${shortSelected ? "Disable" : "Enable"} short arbitrage for ${cycle.underlying} ${intervalLabel(cycle.interval_seconds)}`}
                          aria-pressed={shortSelected}
                          title={
                            cycle.pairs.length
                              ? "Apply this selection when live trading starts"
                              : "A matched route is required"
                          }
                          disabled={
                            shortSelectionLocked ||
                            (!cycle.pairs.length && !shortSelected)
                          }
                          onClick={() => onShortMarketToggle(cycle.monitor_key)}
                        >
                          {shortSelected ? "Short enabled" : "Enable short"}
                        </Button>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
