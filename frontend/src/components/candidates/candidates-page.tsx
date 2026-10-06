import { type FormEvent, useState } from "react"
import { ArrowRight, Search, SearchX, TriangleAlert } from "lucide-react"
import { VenueLogo } from "@/components/venues/venue-logo"

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

/** Round numeric values and API Decimal strings for display without changing source data. */
function formatUsd(value: number | string | null): string {
  const amount = value == null || value === "" ? NaN : Number(value)
  return Number.isFinite(amount)
    ? `$${amount.toLocaleString("en-US", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      })}`
    : "Not supplied"
}

function isLive(candidate: ArbitrageCandidate): boolean {
  if (!candidate.starts_at || !candidate.ends_at) return false
  const now = Date.now()
  return (
    Date.parse(candidate.starts_at) <= now &&
    now < Date.parse(candidate.ends_at)
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
  const [searchDraft, setSearchDraft] = useState("")
  const [searchText, setSearchText] = useState("")
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
    const nextSearch = searchDraft.trim()
    setSearchDraft(nextSearch)
    setSearchText(nextSearch)
  }

  function clearSearch() {
    setSearchDraft("")
    setSearchText("")
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
            reason instanceof Error
              ? reason.message
              : "Candidate monitoring failed",
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
        title="Event catalog"
        description="Find a shared question, compare its venues and connect the markets you want to follow."
      />
      <Card>
        <CardHeader className="gap-5">
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <CardTitle>
                {view === "opportunities"
                  ? "Live arbitrage edges"
                  : "Explore markets"}
              </CardTitle>
              <CardDescription className="mt-2">
                {view === "opportunities"
                  ? "Positive cross-venue returns reported by AGG. Refreshed every 60 seconds."
                  : "Open markets from AGG. Search a policy topic to narrow the catalog."}
              </CardDescription>
            </div>
            <div
              className="flex rounded-lg bg-muted p-1"
              aria-label="Candidate view"
            >
              <Button
                size="sm"
                variant={view === "markets" ? "secondary" : "ghost"}
                aria-pressed={view === "markets"}
                onClick={() => setView("markets")}
              >
                All markets
              </Button>
              <Button
                size="sm"
                variant={view === "opportunities" ? "secondary" : "ghost"}
                aria-pressed={view === "opportunities"}
                onClick={() => setView("opportunities")}
              >
                Live edges
              </Button>
            </div>
          </div>
          <form
            className="flex flex-wrap items-center gap-2"
            onSubmit={submitSearch}
          >
            <div className="relative min-w-40 flex-1">
              <Search
                aria-hidden="true"
                className="absolute left-3 top-3 size-4 text-muted-foreground"
              />
              <label className="sr-only" htmlFor="candidate-search">
                Search candidates
              </label>
              <input
                id="candidate-search"
                type="search"
                value={searchDraft}
                onChange={(event) => setSearchDraft(event.target.value)}
                placeholder="Search an event, e.g. Fed rate hike"
                className="h-10 w-full rounded-lg border border-border bg-background pl-10 pr-3 text-sm outline-none focus:border-primary"
              />
            </div>
            <Button type="submit">Search</Button>
            {searchText && (
              <Button type="button" variant="ghost" onClick={clearSearch}>
                Clear search
              </Button>
            )}
          </form>
          <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
            <div className="flex flex-wrap items-center gap-2">
              <span>Try a topic</span>
              {["Fed", "FOMC", "Trump", "Election"].map((topic) => (
                <button
                  key={topic}
                  type="button"
                  className="rounded-full border border-border px-3 py-1.5 transition-colors hover:border-primary/40 hover:bg-primary/5 hover:text-primary"
                  onClick={() => {
                    setSearchDraft(topic)
                    setSearchText(topic)
                  }}
                >
                  {topic}
                </button>
              ))}
            </div>
            <span>
              {candidates.length} results |{" "}
              {
                candidates.filter(
                  (candidate) => monitorableVenueCount(candidate) >= 2,
                ).length
              }{" "}
              connectable
            </span>
          </div>
        </CardHeader>
        <CardContent>
          {error && (
            <div
              role="alert"
              className="mb-5 rounded-xl border border-amber-400/25 bg-amber-400/5 p-4"
            >
              <div className="flex gap-3">
                <TriangleAlert className="mt-0.5 size-5 shrink-0 text-amber-600 dark:text-amber-300" />
                <div className="min-w-0">
                  <p className="text-sm font-medium">
                    The event source could not complete this request
                  </p>
                  <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                    {candidates.length
                      ? "Showing the last successful results. "
                      : ""}
                    Try another term or browse without a search filter.
                  </p>
                  <details className="mt-3 text-xs text-muted-foreground">
                    <summary className="cursor-pointer">
                      Technical details
                    </summary>
                    <p className="mt-2 break-words">{error}</p>
                  </details>
                  {searchText && (
                    <Button
                      size="sm"
                      variant="outline"
                      className="mt-3"
                      onClick={clearSearch}
                    >
                      Browse all markets
                    </Button>
                  )}
                </div>
              </div>
            </div>
          )}
          {loading && !candidates.length ? (
            <div className="grid gap-4 lg:grid-cols-2">
              <Skeleton className="h-56 w-full" />
              <Skeleton className="h-56 w-full" />
            </div>
          ) : !candidates.length && !error ? (
            <div className="rounded-xl border border-dashed border-border px-5 py-14 text-center">
              <SearchX className="mx-auto mb-4 size-8 text-muted-foreground" />
              <p className="text-base font-medium">
                No {view === "markets" ? "matching events" : "live edges"}{" "}
                available
              </p>
              <p className="mx-auto mt-2 max-w-md text-sm leading-relaxed text-muted-foreground">
                Try another policy term such as FOMC, interest rates, or an
                election name.
              </p>
            </div>
          ) : (
            <div className="grid gap-4 2xl:grid-cols-2">
              {candidates.map((candidate) => {
                const key = candidateKey(candidate)
                const result = monitorResults[key]
                const venueCount = monitorableVenueCount(candidate)
                return (
                  <article
                    key={key}
                    className="flex min-w-0 flex-col rounded-xl border border-border bg-background/35 p-5 transition-colors hover:border-primary/25"
                  >
                    <div className="flex items-start justify-between gap-4">
                      <div className="min-w-0">
                        <p className="text-xs leading-relaxed text-muted-foreground">
                          {candidate.event_title ?? "Market"}
                        </p>
                        <h3 className="mt-1.5 break-words text-base font-semibold leading-snug">
                          {candidate.title}
                        </h3>
                      </div>
                      <Badge
                        variant={
                          candidate.return_rate > 0 ? "positive" : "outline"
                        }
                        className="shrink-0"
                      >
                        {candidate.return_rate > 0
                          ? `${formatPercent(candidate.return_rate)} edge`
                          : "No edge"}
                      </Badge>
                    </div>
                    <div className="mt-4 flex flex-wrap gap-2">
                      {candidate.markets.map((market) => (
                        <span
                          key={`${market.venue_id}:${market.market_id}`}
                          className="inline-flex items-center gap-2 rounded-lg border border-border/70 bg-card px-2.5 py-1.5 text-[11px] font-medium"
                        >
                          <VenueLogo venue={market.venue_id} />
                          {market.venue_id}
                        </span>
                      ))}
                      {isLive(candidate) && (
                        <Badge variant="positive">Live event</Badge>
                      )}
                    </div>
                    <div className="mt-4 flex flex-wrap gap-x-6 gap-y-2 text-xs text-muted-foreground">
                      <span>
                        Combined volume{" "}
                        <strong className="ml-1 font-medium text-foreground">
                          {formatUsd(candidate.volume_usd)}
                        </strong>
                      </span>
                      <span>
                        Observed {formatTimestamp(candidate.observed_at)}
                      </span>
                    </div>
                    <details className="mt-4 text-xs">
                      <summary className="cursor-pointer text-muted-foreground">
                        Venue markets & identifiers
                      </summary>
                      <div className="mt-3 space-y-3">
                        {candidate.markets.map((market) => (
                          <div
                            key={`${market.venue_id}:${market.market_id}`}
                            className="rounded-lg border border-border/70 p-3"
                          >
                            <p className="font-medium">
                              {market.venue_id} |{" "}
                              {market.title ?? "Unnamed market"}
                            </p>
                            <p className="mt-1 break-all font-mono text-[10px] text-muted-foreground">
                              ID {market.external_market_id}
                            </p>
                            <p className="mt-1 text-muted-foreground">
                              Volume {formatUsd(market.volume_usd)}
                            </p>
                          </div>
                        ))}
                        <p className="break-all text-muted-foreground">
                          Event {candidate.venue_event_id ?? "unknown"}
                        </p>
                        {candidate.starts_at && (
                          <p className="text-muted-foreground">
                            Starts {formatTimestamp(candidate.starts_at)}
                          </p>
                        )}
                      </div>
                    </details>
                    <div className="mt-auto pt-5">
                      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border/70 pt-4">
                        <p className="text-xs text-muted-foreground">
                          {venueCount >= 2
                            ? `${venueCount} supported venues`
                            : "Needs at least two supported venues"}
                        </p>
                        <Button
                          size="sm"
                          variant={
                            result?.status === "success"
                              ? "secondary"
                              : "outline"
                          }
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
                          {venueCount >= 2 && !result && (
                            <ArrowRight className="size-3.5" />
                          )}
                        </Button>
                      </div>
                      {result && (
                        <p
                          role={result.status === "error" ? "alert" : "status"}
                          className={`mt-3 break-words text-xs ${result.status === "error" ? "text-rose-700 dark:text-rose-300" : "text-emerald-700 dark:text-emerald-300"}`}
                        >
                          {result.message}
                        </p>
                      )}
                    </div>
                  </article>
                )
              })}
            </div>
          )}
          {updatedAt && (
            <p className="pt-4 text-[11px] text-muted-foreground">
              Last successful refresh {updatedAt.toLocaleTimeString()}
            </p>
          )}
        </CardContent>
      </Card>
    </>
  )
}
