import { Activity, ArrowRight, Clock3, Gauge, Send } from "lucide-react"
import { useState } from "react"
import { StatusCard } from "@/components/dashboard/status-card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { VenueLogo } from "@/components/venues/venue-logo"
import { cn } from "@/lib/utils"
import type { LatencySummary, PipelineMetrics } from "@/types/metrics"
import type { ExecutionLatencyTrace } from "@/types/runtime"

/** Format histogram seconds without rounding sub-millisecond observations to zero. */
export function metricDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds)) return "No samples"
  if (seconds >= 1) return `${seconds.toFixed(2)} s`
  const ms = seconds * 1000
  return `${ms.toLocaleString("en-US", { maximumFractionDigits: ms < 1 ? 3 : 1 })} ms`
}

const intervals = [
  [
    "opportunity_to_both_submits",
    "Both orders submitted",
    "Opportunity detected → both submit calls",
  ],
  [
    "opportunity_to_both_acks",
    "Both venues acknowledge",
    "Opportunity detected → both acknowledgements",
  ],
  [
    "opportunity_to_both_fills",
    "Both legs receive a fill",
    "Opportunity detected → both first fills",
  ],
  [
    "opportunity_to_both_terminal",
    "Both legs reach a result",
    "Opportunity detected → both terminal results",
  ],
  ["submit_start_skew", "Submission gap", "Time between the two submit calls"],
] as const

function TimingBar({
  label,
  detail,
  value,
  maximum,
  count,
}: {
  label: string
  detail: string
  value: number | null
  maximum: number
  count?: number
}) {
  return (
    <div className="grid min-w-0 gap-2 py-3.5 sm:grid-cols-[minmax(0,1fr)_minmax(40px,0.7fr)_88px] sm:items-center sm:gap-3">
      <div>
        <p className="text-sm font-medium">{label}</p>
        <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
          {detail}
        </p>
      </div>
      <div
        aria-hidden="true"
        className="h-2 overflow-hidden rounded-full bg-muted"
      >
        <div
          className="h-full rounded-full bg-primary/75 transition-[width] duration-300"
          style={{
            width: `${value == null || maximum <= 0 ? 0 : Math.max(0, Math.min(100, (value / maximum) * 100))}%`,
          }}
        />
      </div>
      <div className="sm:text-right">
        <p
          className={cn(
            "numeric text-sm font-semibold",
            value == null && "text-xs font-normal text-muted-foreground",
          )}
        >
          {metricDuration(value)}
        </p>
        {count !== undefined && count > 0 && (
          <p className="mt-1 text-[10px] text-muted-foreground">
            {count.toLocaleString()} samples
          </p>
        )}
      </div>
    </div>
  )
}

/** Compare independent latency intervals and venue distributions without summing percentiles. */
export function LatencyOverview({
  pipeline,
  attempt,
}: {
  pipeline: PipelineMetrics
  attempt: ExecutionLatencyTrace | null
}) {
  const [percentile, setPercentile] = useState<"p50" | "p95" | "p99">("p95")
  const [venueView, setVenueView] = useState<"feed" | "book" | "submit">("feed")
  const rows = intervals.map(([stage, label, detail]) => {
    const summary = pipeline.stages
      .filter((item) => item.labels.stage === stage && item.count > 0)
      .reduce<LatencySummary | null>(
        (worst, item) =>
          !worst || item[percentile] > worst[percentile] ? item : worst,
        null,
      )
    return {
      label,
      detail,
      value: summary?.[percentile] ?? null,
      count: summary?.count ?? 0,
    }
  })
  const maximum = Math.max(0, ...rows.map((row) => row.value ?? 0))
  const venues =
    venueView === "feed"
      ? pipeline.marketFeeds.map((feed) => ({
          venue: feed.venue,
          value: feed.receiveToSinkP95,
        }))
      : pipeline.venues.map((venue) => ({
          ...venue,
          value: venueView === "book" ? venue.bookAgeP95 : venue.submitP95,
        }))
  const venueMax = Math.max(0, ...venues.map((venue) => venue.value ?? 0))
  const localStages = [
    ["Read → plan", attempt?.stages.newest_book_to_plan_ms],
    ["Plan → dispatch", attempt?.stages.plan_to_dispatcher_ms],
    ["Prepare requests", attempt?.stages.dispatcher_prepare_ms],
    ["Prepare → guard", attempt?.stages.prepare_to_guard_ms],
    ["Check guards", attempt?.stages.guard_ms],
    ["Guard → submits", attempt?.stages.guard_to_both_submits_ms],
  ] as const
  const localMax = Math.max(0, ...localStages.map(([, value]) => value ?? 0))

  return (
    <div className="space-y-5">
      <section
        className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4"
        aria-label="Latency summary"
      >
        <StatusCard
          label="Both legs terminal · p95"
          value={metricDuration(pipeline.cycleP95)}
          detail="Detection to a final result on both legs"
          icon={Gauge}
        />
        <StatusCard
          label="Both submits · p95"
          value={metricDuration(pipeline.bothSubmitP95)}
          detail="Detection to both submission calls"
          icon={Send}
        />
        <StatusCard
          label="Submission gap · p95"
          value={metricDuration(pipeline.submitSkewP95)}
          detail="Difference between the two start times"
          icon={Clock3}
        />
        <StatusCard
          label="Event loop · p95"
          value={metricDuration(pipeline.eventLoopLagP95)}
          detail="How long scheduled work was delayed"
          icon={Activity}
        />
      </section>

      <Card>
        <CardHeader className="gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <CardTitle>Where does the time go?</CardTitle>
            <CardDescription className="mt-2">
              Compare intervals on the same scale. They overlap and should not
              be added together.
            </CardDescription>
          </div>
          <div
            className="flex w-fit shrink-0 gap-1 rounded-lg bg-muted p-1"
            aria-label="Latency percentile"
          >
            {(["p50", "p95", "p99"] as const).map((value) => (
              <Button
                key={value}
                size="sm"
                variant={value === percentile ? "secondary" : "ghost"}
                aria-pressed={value === percentile}
                onClick={() => setPercentile(value)}
              >
                {value}
              </Button>
            ))}
          </div>
        </CardHeader>
        <CardContent>
          <div className="mb-3 flex flex-wrap gap-x-5 gap-y-2 rounded-lg bg-muted/50 px-4 py-3 text-xs text-muted-foreground">
            <span>
              <strong className="text-foreground">p50</strong> · typical
              observation
            </span>
            <span>
              <strong className="text-foreground">p95</strong> · 95% at or below
              this estimate
            </span>
            <span>
              <strong className="text-foreground">p99</strong> · slower tail
            </span>
          </div>
          <div className="divide-y divide-border/60">
            {rows.map((row) => (
              <TimingBar key={row.label} {...row} maximum={maximum} />
            ))}
          </div>
          {!rows.some((row) => row.count > 0) && (
            <p className="mt-3 rounded-lg border border-dashed border-border p-4 text-xs leading-relaxed text-muted-foreground">
              No execution measurements in this window. These intervals appear
              when an order attempt reaches the corresponding stage; market
              monitoring alone does not generate them.
            </p>
          )}
          <p className="mt-4 text-[11px] text-muted-foreground">
            Histogram estimates for the current window. When multiple series
            exist, the slowest {percentile} is shown.
          </p>
        </CardContent>
      </Card>

      <section className="grid gap-5 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-center justify-between gap-3">
              <CardTitle>Compare venues</CardTitle>
              <Badge variant="outline">p95</Badge>
            </div>
            <CardDescription>
              Feed handoff is local processing. Book age measures freshness;
              submission measures the order request.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div
              className="mb-3 flex w-fit gap-1 rounded-lg bg-muted p-1"
              aria-label="Venue comparison"
            >
              <Button
                size="sm"
                variant={venueView === "feed" ? "secondary" : "ghost"}
                aria-pressed={venueView === "feed"}
                onClick={() => setVenueView("feed")}
              >
                Feed handoff
              </Button>
              <Button
                size="sm"
                variant={venueView === "book" ? "secondary" : "ghost"}
                aria-pressed={venueView === "book"}
                onClick={() => setVenueView("book")}
              >
                Book age
              </Button>
              <Button
                size="sm"
                variant={venueView === "submit" ? "secondary" : "ghost"}
                aria-pressed={venueView === "submit"}
                onClick={() => setVenueView("submit")}
              >
                Submission
              </Button>
            </div>
            {venues.length ? (
              venues.map((venue) => (
                <div
                  key={venue.venue}
                  className="flex items-center gap-3 border-b border-border/60 last:border-0"
                >
                  <VenueLogo venue={venue.venue} />
                  <div className="min-w-0 flex-1">
                    <TimingBar
                      label={venue.venue}
                      detail={
                        venueView === "feed"
                          ? "Receive to engine handoff"
                          : venueView === "book"
                            ? "Order-book age"
                            : "Submit request duration"
                      }
                      value={venue.value}
                      maximum={venueMax}
                    />
                  </div>
                </div>
              ))
            ) : (
              <div className="rounded-xl border border-dashed border-border p-6 text-center">
                <Activity className="mx-auto mb-3 size-6 text-muted-foreground" />
                <p className="text-sm font-medium">
                  Waiting for venue measurements
                </p>
                <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
                  Connect an event to begin observing its feeds.
                </p>
                <a
                  href="/events"
                  className="mt-4 inline-flex items-center gap-2 text-xs text-primary"
                >
                  Open event catalog <ArrowRight className="size-3" />
                </a>
              </div>
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-center justify-between gap-3">
              <CardTitle>Latest order attempt</CardTitle>
              {attempt && (
                <Badge variant={attempt.error ? "warning" : "outline"}>
                  {attempt.outcome.replaceAll("_", " ")}
                </Badge>
              )}
            </div>
            <CardDescription>
              Measured local stages from one correlated attempt, independent of
              the histogram window.
            </CardDescription>
          </CardHeader>
          <CardContent>
            {attempt ? (
              <>
                <div className="divide-y divide-border/60">
                  {localStages.map(([label, value]) => (
                    <TimingBar
                      key={label}
                      label={label}
                      detail="Local processing"
                      value={value == null ? null : value / 1000}
                      maximum={localMax / 1000}
                    />
                  ))}
                </div>
                {attempt.error && (
                  <p className="mt-3 break-words text-xs text-rose-700 dark:text-rose-300">
                    {attempt.error}
                  </p>
                )}
                <p className="mt-3 break-all font-mono text-[10px] text-muted-foreground">
                  {attempt.execution_id}
                </p>
              </>
            ) : (
              <div className="flex min-h-44 flex-col items-center justify-center rounded-xl border border-dashed border-border p-6 text-center">
                <Clock3 className="mb-3 size-6 text-muted-foreground" />
                <p className="text-sm font-medium">No order attempt recorded</p>
                <p className="mt-2 max-w-xs text-xs leading-relaxed text-muted-foreground">
                  A recorded attempt will show its planning, preparation and
                  guard timings here.
                </p>
              </div>
            )}
          </CardContent>
        </Card>
      </section>
    </div>
  )
}
