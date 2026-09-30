import { type FormEvent, useState } from "react"

import { PageHeader } from "@/components/layout/page-header"
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
import {
  type AggDiscoveryView,
  useArbitrageCandidates,
} from "@/features/agg/use-agg-candidates"
import { apiClient } from "@/lib/api-client"
import { formatPercent, formatTimestamp } from "@/lib/formatters"
import type { ArbitrageCandidate } from "@/types/agg"

type MonitorResult = { status: "success" | "error"; message: string }

const MONITORABLE_VENUES = new Set(["POLYMARKET", "LIMITLESS", "PREDICT"])

function candidateKey(candidate: ArbitrageCandidate): string {
  return candidate.markets
    .map(({ venue_id, market_id }) => `${venue_id}:${market_id}`)
    .sort()
    .join("|")
}

function formatUsd(value: number | null): string {
  return value == null
    ? "Not supplied"
    : `$${value.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}`
}

function isLive(candidate: ArbitrageCandidate): boolean {
  if (!candidate.starts_at || !candidate.ends_at) return false
  const now = Date.now()
  return (
    Date.parse(candidate.starts_at) <= now && now < Date.parse(candidate.ends_at)
  )
}

function monitorableVenueCount(candidate: ArbitrageCandidate): number {
  return new Set(
    candidate.markets
      .map(({ venue_id }) => venue_id.toUpperCase())
      .filter((venueId) => MONITORABLE_VENUES.has(venueId)),
  ).size
}

/** Present searchable policy events with explicit multi-venue monitoring. */
export function CandidatesPage() {
  const [view, setView] = useState<AggDiscoveryView>("markets")
  const [searchDraft, setSearchDraft] = useState("Fed")
  const [searchText, setSearchText] = useState("Fed")
  const { candidates, loading, error, updatedAt } = useArbitrageCandidates(
    { searchText },
    view,
  )
  const [monitoring, setMonitoring] = useState<string | null>(null)
  const [monitorResults, setMonitorResults] = useState<
    Record<string, MonitorResult>
  >({})

  function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const nextSearch = searchDraft.trim() || "Fed"
    setSearchDraft(nextSearch)
    setSearchText(nextSearch)
  }

  function clearSearch() {
    setSearchDraft("Fed")
    setSearchText("Fed")
  }

  async function monitor(candidate: ArbitrageCandidate) {
    const key = candidateKey(candidate)
    setMonitoring(key)
    setMonitorResults((current) => {
      const next = { ...current }
      delete next[key]
      return next
    })
    try {
      const result = await apiClient.monitorArbitrageCandidate(candidate)
      setMonitorResults((current) => ({
        ...current,
        [key]: {
          status: "success",
          message: `${result.venue_ids.join(" + ")} · ${result.pair_count} pairs`,
        },
      }))
    } catch (reason) {
      setMonitorResults((current) => ({
        ...current,
        [key]: {
          status: "error",
          message:
            reason instanceof Error ? reason.message : "Candidate monitoring failed",
        },
      }))
    } finally {
      setMonitoring(null)
    }
  }

  return (
    <>
      <PageHeader
        eyebrow="Multi-venue discovery"
        title="Policy events"
        description="Find the same policy question across supported venues and connect its live order books."
      />

      <Card>
        <CardHeader className="gap-4">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <CardTitle>
                {view === "opportunities"
                  ? "Live arbitrage edges"
                  : "Event catalog"}
              </CardTitle>
              <CardDescription>
                {view === "opportunities"
                  ? "Only positive cross-venue returns. Discovery refreshes every 60 seconds."
                  : "Open policy markets, including entries with no current edge or venue match."}
              </CardDescription>
            </div>
            <div
              className="flex rounded-md border border-border p-1"
              aria-label="Candidate view"
            >
              <Button
                size="sm"
                variant={view === "opportunities" ? "secondary" : "ghost"}
                onClick={() => setView("opportunities")}
              >
                Live edges
              </Button>
              <Button
                size="sm"
                variant={view === "markets" ? "secondary" : "ghost"}
                onClick={() => setView("markets")}
              >
                All matches
              </Button>
            </div>
          </div>
          <div className="flex flex-wrap items-center justify-between gap-2">
            <CardDescription>
              Search defaults to the next Fed decision. Select a matched group
              to monitor every compatible venue.
            </CardDescription>
            <form
              className="flex min-w-[280px] flex-1 items-center justify-end gap-2"
              onSubmit={submitSearch}
            >
              <label className="sr-only" htmlFor="candidate-search">
                Search candidates
              </label>
              <input
                id="candidate-search"
                type="search"
                value={searchDraft}
                onChange={(event) => setSearchDraft(event.target.value)}
                placeholder="Search an event, e.g. Fed rate hike"
                className="h-9 min-w-0 flex-1 rounded-md border border-border bg-background px-3 text-sm outline-none transition focus:border-primary sm:max-w-md"
              />
              <Button type="submit" size="sm">
                Search
              </Button>
              {searchText !== "Fed" && (
                <Button type="button" size="sm" variant="ghost" onClick={clearSearch}>
                  Reset to Fed
                </Button>
              )}
            </form>
            <div className="flex w-full items-center justify-end gap-2">
              <Badge variant="outline">
                {candidates.length} {view === "markets" ? "matches" : "edges"}
              </Badge>
            </div>
          </div>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          {loading && !candidates.length ? (
            <div className="space-y-3 px-3 pb-3">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : error && !candidates.length ? (
            <p className="px-3 pb-3 text-xs text-rose-300">{error}</p>
          ) : !candidates.length ? (
            <div className="min-h-56 px-4 py-14 text-center">
              <p className="text-sm font-medium">
                No {view === "markets" ? "matching events" : "live edges"} available
              </p>
              <p className="mx-auto mt-2 max-w-md text-xs leading-relaxed text-muted-foreground">
                Try another policy term such as FOMC, interest rates, or an
                election name.
              </p>
            </div>
          ) : (
            <Table className="min-w-[1100px] table-fixed">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-20">
                    {view === "markets" ? "Edge" : "Return"}
                  </TableHead>
                  <TableHead className="w-64">Market</TableHead>
                  <TableHead className="w-80">Venue matches</TableHead>
                  <TableHead className="w-36">Volume</TableHead>
                  <TableHead className="w-36">Observed</TableHead>
                  <TableHead className="w-36 text-right">Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {candidates.map((candidate) => {
                  const key = candidateKey(candidate)
                  const result = monitorResults[key]
                  const venueCount = monitorableVenueCount(candidate)
                  return (
                    <TableRow key={key}>
                      <TableCell>
                        {candidate.return_rate > 0 ? (
                          <span className="font-semibold text-emerald-300">
                            {formatPercent(candidate.return_rate)}
                          </span>
                        ) : (
                          <span className="text-xs text-muted-foreground">
                            No edge
                          </span>
                        )}
                      </TableCell>
                      <TableCell className="max-w-64 overflow-hidden whitespace-normal">
                        {candidate.event_title && (
                          <span className="line-clamp-2 break-words text-xs text-muted-foreground">
                            {candidate.event_title}
                          </span>
                        )}
                        <div className="flex min-w-0 items-start gap-2">
                          <span
                            className="line-clamp-2 min-w-0 flex-1 break-words font-medium"
                            title={candidate.title}
                          >
                            {candidate.title}
                          </span>
                          {isLive(candidate) && (
                            <Badge className="shrink-0 bg-rose-500/15 text-rose-300">
                              LIVE
                            </Badge>
                          )}
                        </div>
                        <span className="block truncate font-mono text-[10px] text-muted-foreground">
                          event {candidate.venue_event_id ?? "unknown"}
                        </span>
                        {candidate.starts_at && (
                          <span className="block text-[10px] text-muted-foreground">
                            Started {formatTimestamp(candidate.starts_at)}
                          </span>
                        )}
                      </TableCell>
                      <TableCell className="max-w-80 whitespace-normal">
                        <div className="min-w-0 space-y-2">
                          {candidate.markets.map((market) => (
                            <div
                              key={`${market.venue_id}:${market.market_id}`}
                              className="rounded-md border border-border/70 px-2 py-1.5"
                            >
                              <div className="flex items-center gap-2">
                                <Badge variant="outline">{market.venue_id}</Badge>
                                <span className="min-w-0 truncate text-xs font-medium">
                                  {market.title ?? "Unnamed market"}
                                </span>
                              </div>
                              <div className="mt-1 flex min-w-0 gap-2 font-mono text-[9px] text-muted-foreground">
                                <span className="min-w-0 truncate" title={market.external_market_id}>
                                  ID {market.external_market_id}
                                </span>
                                <span>Vol {formatUsd(market.volume_usd)}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </TableCell>
                      <TableCell>
                        <span className="font-medium">
                          {formatUsd(candidate.volume_usd)}
                        </span>
                        <span className="block text-[10px] text-muted-foreground">
                          combined venue volume
                        </span>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {formatTimestamp(candidate.observed_at)}
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={
                            venueCount < 2 ||
                            monitoring !== null ||
                            result?.status === "success"
                          }
                          onClick={() => void monitor(candidate)}
                        >
                          {venueCount < 2
                            ? "Needs match"
                            : monitoring === key
                            ? "Connecting..."
                            : result?.status === "success"
                              ? "Monitoring"
                              : "Monitor"}
                        </Button>
                        {result && (
                          <span
                            className={`mt-2 block max-w-48 text-[10px] ${
                              result.status === "error"
                                ? "text-rose-300"
                                : "text-emerald-300"
                            }`}
                          >
                            {result.message}
                          </span>
                        )}
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          )}
          {updatedAt && (
            <p className="px-3 pt-3 text-[10px] text-muted-foreground">
              Last refreshed {updatedAt.toLocaleTimeString()}
            </p>
          )}
        </CardContent>
      </Card>
    </>
  )
}
