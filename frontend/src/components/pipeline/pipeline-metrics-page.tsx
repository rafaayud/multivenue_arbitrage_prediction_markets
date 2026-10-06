import { Activity, CircleGauge, Send, TriangleAlert } from "lucide-react"
import { useState } from "react"
import { NavLink } from "react-router-dom"
import { Badge } from "@/components/ui/badge"
import { PipelineOverview } from "@/components/pipeline/pipeline-overview"
import { LatencyOverview } from "@/components/pipeline/latency-overview"

import { StatusCard } from "@/components/dashboard/status-card"
import { PageHeader } from "@/components/layout/page-header"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { VenueLogo } from "@/components/venues/venue-logo"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { useExecutionActivity } from "@/features/execution/execution-activity-provider"
import { useLatencyMetrics } from "@/features/metrics/use-latency-metrics"
import { useRuntime } from "@/features/runtime/use-runtime"
import { formatLatency, formatQuantity } from "@/lib/formatters"
import type { FillCaptureSummary, LatencySummary } from "@/types/metrics"
import type { ExecutionLatencyTrace } from "@/types/runtime"

const stageNames: Record<string, string> = {
  opportunity_to_submit: "Opportunity → leg submit",
  submit_start_skew: "Submit start skew",
  opportunity_to_both_submits: "Opportunity → both submits",
  submit_to_ack: "Leg submit → venue ack",
  opportunity_to_both_acks: "Opportunity → both acks",
  ack_to_first_fill: "Venue ack → first fill",
  opportunity_to_both_fills: "Opportunity → both fills",
  ack_to_terminal: "Venue ack → terminal result",
  terminal_skew: "Terminal result skew",
  opportunity_to_both_terminal: "Opportunity → both terminal",
  watch: "Private stream readiness",
  adapter_prepare: "Build and sign venue payload",
  journal_append: "Persist prepared order",
  prepare_total: "Total leg preparation",
  entry_pricing_fixed_ticks: "Entry sizing + fixed ticks (within book → plan)",
  entry_pricing_edge_budget: "Entry sizing + edge budget (within book → plan)",
}

const attemptStages: Array<{
  key: keyof ExecutionLatencyTrace["stages"]
  label: string
  scope: string
}> = [
  {
    key: "book_arrival_skew_ms",
    label: "Book arrival skew",
    scope: "Feed / network",
  },
  {
    key: "newest_book_to_plan_ms",
    label: "Newest local book -> plan",
    scope: "Local",
  },
  {
    key: "plan_to_dispatcher_ms",
    label: "Plan -> dispatcher pair",
    scope: "Local",
  },
  {
    key: "dispatcher_prepare_ms",
    label: "Parallel request preparation",
    scope: "Local",
  },
  {
    key: "prepare_to_guard_ms",
    label: "Prepared pair -> guard check",
    scope: "Local",
  },
  { key: "guard_ms", label: "Guard execution", scope: "Local" },
  {
    key: "guard_to_both_submits_ms",
    label: "Guard -> both submit calls",
    scope: "Local",
  },
]

function latency(value: number | null): string {
  return value === null ? "—" : formatLatency(value)
}

function milliseconds(value: number | null): string {
  return value === null ? "—" : `${value.toFixed(value >= 10 ? 2 : 3)} ms`
}

function venueLabel(value: string | undefined): string {
  if (!value) return "—"
  return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase()
}

type StageRow = Pick<
  LatencySummary,
  "id" | "labels" | "count" | "p999" | "maximumLowerBound" | "maximumUpperBound"
> & {
  p50: number | null
  p95: number | null
  p99: number | null
}

function worstLatency(
  summaries: LatencySummary[],
  metric: string,
  predicate: (summary: LatencySummary) => boolean = () => true,
): LatencySummary | null {
  return (
    summaries
      .filter((summary) => summary.metric === metric && predicate(summary))
      .sort((left, right) => {
        const upper = (summary: LatencySummary) =>
          summary.maximumUpperBound ?? Number.POSITIVE_INFINITY
        return (
          upper(right) - upper(left) ||
          right.maximumLowerBound - left.maximumLowerBound
        )
      })[0] ?? null
  )
}

function maximumBucket(
  summary: Pick<
    LatencySummary,
    "maximumLowerBound" | "maximumUpperBound"
  > | null,
): string {
  if (!summary) return "—"
  return summary.maximumUpperBound === null
    ? `>${formatLatency(summary.maximumLowerBound)}`
    : `≤${formatLatency(summary.maximumUpperBound)}`
}

function ageLabel(seconds: number | null): string {
  if (seconds === null) return "No open residuals"
  if (seconds < 60) return `${Math.floor(seconds)}s oldest`
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m oldest`
  return `${Math.floor(seconds / 3_600)}h oldest`
}

function memory(value: number | null): string {
  return value === null ? "—" : `${(value / 1024 / 1024).toFixed(1)} MiB`
}

function captureLabel(capture: FillCaptureSummary): string {
  if (capture.state === "running" && capture.writerAlive === false)
    return "Writer stopped"
  return (
    (
      {
        disabled: "Disabled",
        running: "Recording",
        closed: "Closed",
        disk_limit: "Capacity exhausted",
        io_error: "Writer error",
        start_failed: "Unavailable",
      } as Record<string, string>
    )[capture.state] ?? "Unknown"
  )
}

function captureTone(
  capture: FillCaptureSummary,
): "negative" | "warning" | "neutral" {
  if (["disk_limit", "io_error", "start_failed"].includes(capture.state))
    return "negative"
  return capture.state === "unknown" ||
    (capture.droppedSamples ?? 0) > 0 ||
    (capture.offersAfterStop ?? 0) > 0 ||
    (capture.state === "running" && capture.writerAlive === false)
    ? "warning"
    : "neutral"
}

/** Diagnose the parallel two-leg execution path from live and persisted metrics. */
export function PipelineMetricsPage({
  view = "pipeline",
}: {
  view?: "pipeline" | "latency"
}) {
  const metrics = useLatencyMetrics()
  const runtime = useRuntime()
  const { snapshot } = useExecutionActivity()
  const persistedAttempt =
    snapshot?.journals.find((journal) => journal.latencyTrace)?.latencyTrace ??
    null
  const attempt = runtime.runtime?.last_execution_latency ?? persistedAttempt
  const [venueMetricView, setVenueMetricView] = useState<"book" | "submit">(
    "book",
  )
  const residuals =
    snapshot?.journals.filter((journal) => journal.residualQuantity > 0) ?? []
  const residualQuantity = residuals.reduce(
    (total, journal) => total + journal.residualQuantity,
    0,
  )
  const oldestResidualAge = residuals.length
    ? Math.max(
        ...residuals.map(
          (journal) =>
            (Date.now() - new Date(journal.updatedAt).getTime()) / 1_000,
        ),
      )
    : null
  const stageRows: StageRow[] = Object.keys(stageNames).flatMap((stage) => {
    const samples = metrics.pipeline.stages.filter(
      (summary) => summary.labels.stage === stage,
    )
    const rows: StageRow[] = samples.length
      ? samples.map((sample) => sample)
      : [
          {
            id: `missing:${stage}`,
            labels: { stage },
            p50: null,
            p95: null,
            p99: null,
            p999: null,
            maximumLowerBound: 0,
            maximumUpperBound: null,
            count: 0,
          },
        ]
    return rows
  })
  const polymarketWs = metrics.pipeline.polymarketWs
  const latencyWindow =
    metrics.pipeline.windowSeconds === null
      ? "since process start"
      : `last ${metrics.pipeline.windowSeconds.toFixed(1)}s`
  const cycleTail = worstLatency(
    metrics.pipeline.latencies,
    "arbitrage_stage_seconds",
    (summary) => summary.labels.stage === "opportunity_to_both_terminal",
  )
  const bothSubmitTail = worstLatency(
    metrics.pipeline.latencies,
    "arbitrage_stage_seconds",
    (summary) => summary.labels.stage === "opportunity_to_both_submits",
  )
  const submitSkewTail = worstLatency(
    metrics.pipeline.latencies,
    "arbitrage_stage_seconds",
    (summary) => summary.labels.stage === "submit_start_skew",
  )
  const messageAgeTail = worstLatency(
    metrics.pipeline.latencies,
    "polymarket_ws_message_age_seconds",
    (summary) => summary.labels.event_type === "price_change",
  )
  const queueWaitTail = worstLatency(
    metrics.pipeline.latencies,
    "polymarket_ws_message_queue_wait_seconds",
  )
  const dequeueToEmitTail = worstLatency(
    metrics.pipeline.latencies,
    "polymarket_ws_dequeue_to_emit_seconds",
  )
  const eventLoopTail = worstLatency(
    metrics.pipeline.latencies,
    "trading_event_loop_lag_seconds",
  )
  const marketWorkers =
    runtime.runtime?.market_worker_mode === "disabled"
      ? []
      : metrics.pipeline.marketWorkers
  const parentCapture = metrics.pipeline.parentCapture
  const aliveWorkers = marketWorkers.filter((worker) => worker.alive).length
  const polymarketWsUnhealthy =
    (polymarketWs.activeSockets !== null &&
      polymarketWs.expectedSockets !== null &&
      polymarketWs.activeSockets !== polymarketWs.expectedSockets) ||
    (polymarketWs.expectedSockets !== null &&
      polymarketWs.expectedSockets > 0 &&
      polymarketWs.activePumps !== null &&
      polymarketWs.activePumps !== 1) ||
    (polymarketWs.pausedSockets ?? 0) > 0

  return (
    <>
      <PageHeader
        eyebrow="Observability"
        title={view === "pipeline" ? "Pipeline" : "Latency"}
        description={
          view === "pipeline"
            ? "See where data is flowing, what is waiting and what needs attention."
            : "Understand response times, compare venues and inspect the latest order attempt."
        }
        actions={
          <Badge
            variant={
              metrics.error
                ? "warning"
                : metrics.loading
                  ? "outline"
                  : "positive"
            }
          >
            {metrics.error
              ? "Metrics unavailable"
              : metrics.loading
                ? "Loading metrics"
                : "Metrics connected"}
          </Badge>
        }
      />
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3 border-b border-border pb-3">
        <nav
          aria-label="Observability views"
          className="flex gap-1 rounded-xl bg-muted/70 p-1"
        >
          {[
            { to: "/pipeline", label: "Pipeline", icon: Activity },
            { to: "/latency", label: "Latency", icon: CircleGauge },
          ].map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors ${isActive ? "bg-card text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"}`
              }
            >
              <Icon className="size-4" />
              {label}
            </NavLink>
          ))}
        </nav>
        <p className="text-[11px] text-muted-foreground">
          Metrics every 10s |{" "}
          {metrics.updatedAt
            ? `updated ${metrics.updatedAt.toLocaleTimeString()}`
            : "waiting for first update"}
          {view === "latency" && ` | ${latencyWindow}`}
        </p>
      </div>
      {(metrics.error || runtime.error) && (
        <div
          role="alert"
          className="mb-5 rounded-xl border border-rose-400/25 bg-rose-400/5 p-4 text-sm"
        >
          <p className="font-medium">Some telemetry is unavailable</p>
          <p className="mt-1 break-words text-xs text-muted-foreground">
            {metrics.error ?? runtime.error} | Previously received values may be
            stale.
          </p>
        </div>
      )}
      {view === "pipeline" ? (
        <PipelineOverview
          runtime={runtime.runtime}
          pipeline={metrics.pipeline}
          unavailable={Boolean(metrics.error || runtime.error)}
        />
      ) : (
        <LatencyOverview pipeline={metrics.pipeline} attempt={attempt} />
      )}
      <details className="diagnostic-details mt-6 rounded-2xl border border-border bg-card">
        <summary>
          {view === "pipeline"
            ? "Advanced diagnostics: sockets, workers & capture"
            : "Detailed measurements: percentiles, checkpoints & failures"}
        </summary>
        <div className="px-4 pb-4 sm:px-5">
          {view === "pipeline" ? (
            <>
              <section className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <StatusCard
                  label="Polymarket WS queue"
                  value={
                    metrics.pipeline.polymarketWs.queueDepth === null
                      ? "-"
                      : `${metrics.pipeline.polymarketWs.queueDepth} frames`
                  }
                  detail={
                    metrics.pipeline.polymarketWs.queueHighWatermark === null
                      ? "Peak unavailable"
                      : `Peak ${metrics.pipeline.polymarketWs.queueHighWatermark} | ${polymarketWs.framesPerSecond?.toFixed(1) ?? "—"} frames/s · ${polymarketWs.booksPerSecond?.toFixed(1) ?? "—"} books/s | ${polymarketWs.queueOverloadsDrained} drained / ${polymarketWs.queueOverloadsRestarted} stalled`
                  }
                  icon={Activity}
                  tone={
                    (metrics.pipeline.polymarketWs.queueDepth ?? 0) >= 192
                      ? "negative"
                      : "neutral"
                  }
                />
                <StatusCard
                  label="Polymarket sockets"
                  value={
                    polymarketWs.activeSockets === null ||
                    polymarketWs.expectedSockets === null
                      ? "-"
                      : `${polymarketWs.activeSockets}/${polymarketWs.expectedSockets}`
                  }
                  detail={`${polymarketWs.pausedSockets ?? "-"} paused | ${polymarketWs.activePumps ?? "-"} pumps | ${polymarketWs.connections ?? "-"} connects`}
                  icon={TriangleAlert}
                  tone={polymarketWsUnhealthy ? "negative" : "neutral"}
                />
                <StatusCard
                  label="Venue message age max bucket"
                  value={maximumBucket(messageAgeTail)}
                  detail={`p99 ${latency(polymarketWs.messageAgeP99)} | latest signed delta ${latency(polymarketWs.priceChangeTimestampDelta)} | ${polymarketWs.desyncs} desync / ${polymarketWs.messageAgeResyncs} stale`}
                  icon={CircleGauge}
                  tone={
                    (metrics.pipeline.polymarketWs.messageAgeP99 ?? 0) >= 2
                      ? "negative"
                      : (metrics.pipeline.polymarketWs.messageAgeP99 ?? 0) > 1
                        ? "warning"
                        : "neutral"
                  }
                />
                <StatusCard
                  label="WS internal queue wait max bucket"
                  value={maximumBucket(queueWaitTail)}
                  detail={
                    polymarketWs.queueWaitConditionId
                      ? `p99 ${latency(polymarketWs.queueWaitP99)} | worst socket ${polymarketWs.queueWaitConditionId.slice(0, 10)}...`
                      : "Assembler enqueue to adapter dequeue"
                  }
                  icon={Activity}
                  tone={
                    (polymarketWs.queueWaitP99 ?? 0) > 0.1
                      ? "negative"
                      : (polymarketWs.queueWaitP99 ?? 0) > 0.05
                        ? "warning"
                        : "neutral"
                  }
                />
                <StatusCard
                  label="WS dequeue to emit max bucket"
                  value={maximumBucket(dequeueToEmitTail)}
                  detail={
                    polymarketWs.booksEmitted === null ||
                    polymarketWs.framesReceived === null
                      ? "Adapter processing and burst coalescing"
                      : `p99 ${latency(polymarketWs.dequeueToEmitP99)} | ${polymarketWs.booksEmitted.toLocaleString()}/${polymarketWs.framesReceived.toLocaleString()} books/frames`
                  }
                  icon={Send}
                />
              </section>

              <section className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <StatusCard
                  label="Event-loop lag max bucket"
                  value={maximumBucket(eventLoopTail)}
                  detail={`p95 ${latency(metrics.pipeline.eventLoopLagP95)} · missed scheduled probes included`}
                  icon={Activity}
                  tone={
                    (metrics.pipeline.eventLoopLagP95 ?? 0) > 0.1
                      ? "negative"
                      : "neutral"
                  }
                />
                {metrics.pipeline.marketFeeds.map((feed) => {
                  const responseTail = worstLatency(
                    metrics.pipeline.latencies,
                    "market_feed_receive_to_sink_seconds",
                    (summary) =>
                      venueLabel(summary.labels.venue) === feed.venue,
                  )
                  const venueAgeTail = worstLatency(
                    metrics.pipeline.latencies,
                    "market_feed_venue_age_seconds",
                    (summary) =>
                      venueLabel(summary.labels.venue) === feed.venue,
                  )
                  return (
                    <StatusCard
                      key={feed.venue}
                      label={`${feed.venue} response max bucket`}
                      value={maximumBucket(responseTail)}
                      detail={`receive→sink p95 ${latency(feed.receiveToSinkP95)} | venue age max ${maximumBucket(venueAgeTail)} | WS ${feed.wsQueueDepth ?? "-"}/${feed.wsQueueHighWatermark ?? "-"} | wait p95 ${latency(feed.wsQueueWaitP95)}`}
                      icon={Send}
                      tone={
                        feed.wsPaused ||
                        (feed.venueAgeP99 ?? 0) > 0.5 ||
                        (feed.wsQueueDepth ?? 0) > 64
                          ? "negative"
                          : "neutral"
                      }
                    />
                  )
                })}
              </section>

              <section className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <StatusCard
                  label="Market-data workers"
                  value={
                    marketWorkers.length
                      ? `${aliveWorkers}/${marketWorkers.length}`
                      : "Disabled"
                  }
                  detail="Isolated public feeds and local detection"
                  icon={Activity}
                  tone={
                    marketWorkers.length &&
                    aliveWorkers !== marketWorkers.length
                      ? "negative"
                      : "neutral"
                  }
                />
                <StatusCard
                  label="Opportunity IPC queue"
                  value={
                    metrics.pipeline.workerIpc.depth === null
                      ? "—"
                      : `${metrics.pipeline.workerIpc.depth}/${metrics.pipeline.workerIpc.capacity ?? "—"}`
                  }
                  detail={`Sampled peak ${metrics.pipeline.workerIpc.highWatermark ?? "—"}`}
                  icon={Send}
                  tone={
                    metrics.pipeline.workerIpc.depth !== null &&
                    metrics.pipeline.workerIpc.capacity !== null &&
                    metrics.pipeline.workerIpc.depth >=
                      metrics.pipeline.workerIpc.capacity * 0.75
                      ? "negative"
                      : "neutral"
                  }
                />
                <StatusCard
                  label="Predict capture · parent"
                  value={
                    metrics.error ? "Unknown" : captureLabel(parentCapture)
                  }
                  detail={`Queue ${parentCapture.queueDepth ?? "—"}/${parentCapture.queueCapacity ?? "—"} | ${parentCapture.droppedSamples ?? "—"} dropped | ${parentCapture.offersAfterStop ?? "—"} after stop | ${memory(parentCapture.bytes)}`}
                  icon={Activity}
                  tone={metrics.error ? "warning" : captureTone(parentCapture)}
                />
              </section>

              <section className="mt-4">
                <Card>
                  <CardHeader>
                    <CardTitle>Worker and IPC telemetry</CardTitle>
                    <CardDescription>
                      One-second child aggregates plus immediate pause,
                      overload, and resync transitions. Deployment values remain
                      provisional until verified in the rebuilt container.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="px-2 sm:px-5">
                    {!marketWorkers.length ? (
                      <p className="py-6 text-center text-sm text-muted-foreground">
                        Multiprocessing is disabled or no worker samples have
                        arrived.
                      </p>
                    ) : (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Partition</TableHead>
                            <TableHead>Process</TableHead>
                            <TableHead className="text-right">
                              Loop p95 / max
                            </TableHead>
                            <TableHead className="text-right">
                              Intent IPC p95 / max
                            </TableHead>
                            <TableHead>Final validation</TableHead>
                            <TableHead className="text-right">
                              Book source p95 / max
                            </TableHead>
                            <TableHead>Capture (last heartbeat)</TableHead>
                            <TableHead>Pressure</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {marketWorkers.map((worker) => {
                            const workerLatency = (metric: string) =>
                              worstLatency(
                                metrics.pipeline.latencies,
                                metric,
                                (summary) =>
                                  summary.labels.partition === worker.partition,
                              )
                            return (
                              <TableRow key={worker.partition}>
                                <TableCell className="font-medium">
                                  {worker.partition}
                                </TableCell>
                                <TableCell className="text-xs text-muted-foreground">
                                  {worker.alive ? "up" : "down"} · PID{" "}
                                  {worker.pid ?? "—"} · gen{" "}
                                  {worker.generation ?? "—"}
                                  <br />
                                  heartbeat {latency(worker.heartbeatAge)} · CPU{" "}
                                  {worker.cpuSeconds?.toFixed(1) ?? "—"}s ·{" "}
                                  {memory(worker.memoryBytes)} ·{" "}
                                  {worker.restarts} restarts
                                </TableCell>
                                <TableCell className="numeric text-right font-mono text-xs">
                                  {latency(worker.eventLoopLagP95)}
                                  <br />
                                  <span className="text-muted-foreground">
                                    {maximumBucket(
                                      workerLatency(
                                        "market_worker_event_loop_lag_seconds",
                                      ),
                                    )}
                                  </span>
                                </TableCell>
                                <TableCell className="numeric text-right font-mono text-xs">
                                  {latency(worker.intentIpcP95)}
                                  <br />
                                  <span className="text-muted-foreground">
                                    {maximumBucket(
                                      workerLatency(
                                        "market_worker_intent_ipc_seconds",
                                      ),
                                    )}{" "}
                                    max · {worker.rejectedIntents} rejected
                                  </span>
                                </TableCell>
                                <TableCell className="text-xs text-muted-foreground">
                                  p95 {latency(worker.validationP95)} · queue{" "}
                                  {worker.validationQueueDepth ?? "—"}/
                                  {worker.validationQueueCapacity ?? "—"}
                                  <br />
                                  {worker.validationOutcomes === null
                                    ? "No validation samples"
                                    : worker.validationOutcomes
                                        .map(
                                          ({ outcome, count }) =>
                                            `${outcome.replaceAll("_", " ")}: ${count}`,
                                        )
                                        .join(" · ") || "No validations"}
                                </TableCell>
                                <TableCell className="numeric text-right font-mono text-xs">
                                  {latency(worker.submissionSourceAgeP95)}
                                  <br />
                                  <span className="text-muted-foreground">
                                    {maximumBucket(
                                      worstLatency(
                                        metrics.pipeline.latencies,
                                        "market_worker_book_age_seconds",
                                        (summary) =>
                                          summary.labels.partition ===
                                            worker.partition &&
                                          summary.labels.stage ===
                                            "submission" &&
                                          summary.labels.clock === "source",
                                      ),
                                    )}
                                  </span>
                                </TableCell>
                                <TableCell className="text-xs text-muted-foreground">
                                  <span
                                    className={
                                      captureTone(worker.capture) === "negative"
                                        ? "text-rose-300"
                                        : captureTone(worker.capture) ===
                                            "warning"
                                          ? "text-amber-300"
                                          : ""
                                    }
                                  >
                                    {metrics.error || !worker.alive
                                      ? "Unknown"
                                      : captureLabel(worker.capture)}
                                  </span>
                                  <br />
                                  queue {worker.capture.queueDepth ?? "—"}/
                                  {worker.capture.queueCapacity ?? "—"} ·{" "}
                                  {worker.capture.droppedSamples ?? "—"} dropped
                                  <br />
                                  {worker.capture.offersAfterStop ?? "—"} after
                                  stop · {worker.capture.activeWindows ?? "—"}{" "}
                                  windows · {memory(worker.capture.bytes)}
                                  <br />
                                  writer idle{" "}
                                  {latency(worker.capture.writerProgressAge)}
                                </TableCell>
                                <TableCell className="text-xs text-muted-foreground">
                                  {worker.venues.length
                                    ? worker.venues
                                        .map(
                                          (venue) =>
                                            `${venue.venue} ${venue.queueDepth ?? "—"}/${venue.queueHighWatermark ?? "—"}, wait max ${latency(venue.queueWaitMax)}${(venue.pausedStreams ?? 0) > 0 ? " paused" : ""}`,
                                        )
                                        .join(" · ")
                                    : "No queue samples"}
                                </TableCell>
                              </TableRow>
                            )
                          })}
                        </TableBody>
                      </Table>
                    )}
                  </CardContent>
                </Card>
              </section>
            </>
          ) : (
            <>
              <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <StatusCard
                  label="Full cycle max bucket"
                  value={maximumBucket(cycleTail)}
                  detail={`p99 ${latency(cycleTail?.p99 ?? null)} · ${cycleTail?.count ?? 0} completed cycles`}
                  icon={CircleGauge}
                />
                <StatusCard
                  label="Both submits max bucket"
                  value={maximumBucket(bothSubmitTail)}
                  detail={`p99 ${latency(bothSubmitTail?.p99 ?? null)} · opportunity to both calls`}
                  icon={Send}
                />
                <StatusCard
                  label="Submit skew max bucket"
                  value={maximumBucket(submitSkewTail)}
                  detail={`p99 ${latency(submitSkewTail?.p99 ?? null)} · start-time difference`}
                  icon={Activity}
                  tone="warning"
                />
                <StatusCard
                  label="Residual exposure"
                  value={snapshot ? formatQuantity(residualQuantity) : "—"}
                  detail={
                    snapshot
                      ? ageLabel(oldestResidualAge)
                      : "Journal unavailable"
                  }
                  icon={TriangleAlert}
                  tone={residualQuantity > 0 ? "negative" : "neutral"}
                />
              </section>

              <section className="mt-4">
                <Card>
                  <CardHeader>
                    <CardTitle>Latest correlated order attempt</CardTitle>
                    <CardDescription>
                      {attempt
                        ? `${attempt.execution_id} · ${attempt.outcome.replaceAll("_", " ")}`
                        : runtime.error
                          ? `Runtime unavailable: ${runtime.error}`
                          : "No order attempt has been recorded."}
                    </CardDescription>
                  </CardHeader>
                  {attempt && (
                    <CardContent className="space-y-4 px-2 sm:px-5">
                      {attempt.error && (
                        <p className="rounded-lg border border-rose-400/20 bg-rose-400/8 p-3 text-xs text-rose-300">
                          {attempt.error}
                        </p>
                      )}
                      <div className="grid gap-4 xl:grid-cols-2">
                        <div>
                          <p className="px-3 pb-2 text-xs font-semibold">
                            Guard-age decomposition
                          </p>
                          <Table>
                            <TableHeader>
                              <TableRow>
                                <TableHead>Stage</TableHead>
                                <TableHead>Scope</TableHead>
                                <TableHead className="text-right">
                                  Latency
                                </TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {attemptStages.map((stage) => (
                                <TableRow key={stage.key}>
                                  <TableCell className="text-xs font-medium">
                                    {stage.label}
                                  </TableCell>
                                  <TableCell className="text-xs text-muted-foreground">
                                    {stage.scope}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(attempt.stages[stage.key])}
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                        <div>
                          <p className="px-3 pb-2 text-xs font-semibold">
                            Per-leg checkpoints
                          </p>
                          <Table>
                            <TableHeader>
                              <TableRow>
                                <TableHead>Leg</TableHead>
                                <TableHead>Venue</TableHead>
                                <TableHead className="text-right">
                                  Book at plan
                                </TableHead>
                                <TableHead className="text-right">
                                  Book at guard
                                </TableHead>
                                <TableHead className="text-right">
                                  Watch
                                </TableHead>
                                <TableHead className="text-right">
                                  Build/sign
                                </TableHead>
                                <TableHead className="text-right">
                                  Journal
                                </TableHead>
                                <TableHead className="text-right">
                                  Prepare total
                                </TableHead>
                                <TableHead className="text-right">
                                  Guard to submit
                                </TableHead>
                                <TableHead className="text-right">
                                  Submit to ack
                                </TableHead>
                                <TableHead className="text-right">
                                  Ack to fill
                                </TableHead>
                                <TableHead className="text-right">
                                  Ack to terminal
                                </TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {attempt.legs.map((leg) => (
                                <TableRow key={leg.role}>
                                  <TableCell className="text-xs font-medium">
                                    {leg.role}
                                  </TableCell>
                                  <TableCell className="text-xs text-muted-foreground">
                                    {leg.venue ?? "—"}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.book_age_at_plan_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.book_age_at_guard_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.watch_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.adapter_prepare_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.journal_append_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.prepare_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.guard_to_submit_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.submit_to_ack_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.ack_to_first_fill_ms)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {milliseconds(leg.ack_to_terminal_ms)}
                                  </TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        </div>
                      </div>
                      <p className="px-3 pb-2 text-xs text-muted-foreground">
                        {attempt.legs.some(
                          (leg) => leg.book_replaced_before_guard,
                        )
                          ? "A book changed after planning, so its guard age does not equal the phase sum above."
                          : attempt.older_book_role
                            ? `Arrival skew applies to the ${attempt.older_book_role} book. Its guard age equals that skew plus the local stages through the guard check.`
                            : "Both books arrived together; guard age is explained by the local stages through the guard check."}
                      </p>
                    </CardContent>
                  )}
                </Card>
              </section>

              <section className="mt-4">
                <Card>
                  <CardHeader>
                    <CardTitle>Critical path latency</CardTitle>
                    <CardDescription>
                      Per-window distribution; max is the narrowest retained
                      histogram bucket, not an invented exact value.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="px-2 sm:px-5">
                    {metrics.loading && !metrics.pipeline.stages.length ? (
                      <div className="space-y-3 px-3 pb-3">
                        <Skeleton className="h-8 w-full" />
                        <Skeleton className="h-8 w-5/6" />
                      </div>
                    ) : (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Stage</TableHead>
                            <TableHead>Venue</TableHead>
                            <TableHead>Leg</TableHead>
                            <TableHead className="text-right">p50</TableHead>
                            <TableHead className="text-right">p95</TableHead>
                            <TableHead className="text-right">p99</TableHead>
                            <TableHead className="text-right">p99.9</TableHead>
                            <TableHead className="text-right">
                              Max bucket
                            </TableHead>
                            <TableHead className="text-right">
                              Samples
                            </TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {stageRows.map((stage) => (
                            <TableRow key={stage.id}>
                              <TableCell className="text-xs font-medium">
                                {stageNames[stage.labels.stage ?? ""] ??
                                  stage.labels.stage}
                              </TableCell>
                              <TableCell className="text-xs text-muted-foreground">
                                {stage.labels.venue ? (
                                  <span className="flex items-center gap-2">
                                    <VenueLogo venue={stage.labels.venue} />
                                    {venueLabel(stage.labels.venue)}
                                  </span>
                                ) : (
                                  "—"
                                )}
                              </TableCell>
                              <TableCell className="text-xs text-muted-foreground">
                                {stage.labels.leg ?? "—"}
                              </TableCell>
                              <TableCell className="numeric text-right font-mono text-xs">
                                {latency(stage.p50)}
                              </TableCell>
                              <TableCell className="numeric text-right font-mono text-xs text-amber-300">
                                {latency(stage.p95)}
                              </TableCell>
                              <TableCell className="numeric text-right font-mono text-xs text-rose-300">
                                {latency(stage.p99)}
                              </TableCell>
                              <TableCell className="numeric text-right font-mono text-xs text-rose-300">
                                {latency(stage.p999)}
                              </TableCell>
                              <TableCell className="numeric text-right font-mono text-xs text-rose-300">
                                {stage.count ? maximumBucket(stage) : "—"}
                              </TableCell>
                              <TableCell className="numeric text-right text-xs text-muted-foreground">
                                {stage.count}
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    )}
                  </CardContent>
                </Card>
              </section>

              <section className="mt-4 grid gap-4 xl:grid-cols-12">
                <Card className="xl:col-span-8">
                  <CardHeader className="flex-row items-start justify-between gap-4">
                    <div>
                      <CardTitle>Venue matrix</CardTitle>
                      <CardDescription>
                        {venueMetricView === "book"
                          ? "Order-book age percentiles by venue."
                          : "Submit latency percentiles by venue."}
                      </CardDescription>
                    </div>
                    <label className="flex shrink-0 items-center gap-2 text-xs text-muted-foreground">
                      <span className="sr-only">Metric set</span>
                      <select
                        aria-label="Metric set"
                        value={venueMetricView}
                        onChange={(event) =>
                          setVenueMetricView(
                            event.target.value as "book" | "submit",
                          )
                        }
                        className="h-8 rounded-lg border border-border bg-background px-2 text-xs font-medium text-foreground outline-none focus:ring-2 focus:ring-ring"
                      >
                        <option value="book">Book</option>
                        <option value="submit">Submit</option>
                      </select>
                    </label>
                  </CardHeader>
                  <CardContent className="px-2 sm:px-5">
                    {!metrics.pipeline.venues.length ? (
                      <p className="px-3 pb-4 text-xs text-muted-foreground">
                        No venue-level samples have been recorded yet.
                      </p>
                    ) : (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Venue</TableHead>
                            {venueMetricView === "book" ? (
                              <>
                                <TableHead className="text-right">
                                  Book age p50
                                </TableHead>
                                <TableHead className="text-right">
                                  Book age p95
                                </TableHead>
                                <TableHead className="text-right">
                                  Book age p99
                                </TableHead>
                                <TableHead className="text-right">
                                  Max bucket
                                </TableHead>
                              </>
                            ) : (
                              <>
                                <TableHead className="text-right">
                                  Submit p50
                                </TableHead>
                                <TableHead className="text-right">
                                  Submit p95
                                </TableHead>
                                <TableHead className="text-right">
                                  Submit p99
                                </TableHead>
                                <TableHead className="text-right">
                                  Max bucket
                                </TableHead>
                              </>
                            )}
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {metrics.pipeline.venues.map((venue) => (
                            <TableRow key={venue.venue}>
                              <TableCell className="text-xs font-medium">
                                <span className="flex items-center gap-2">
                                  <VenueLogo venue={venue.venue} />
                                  {venue.venue}
                                </span>
                              </TableCell>
                              {venueMetricView === "book" ? (
                                <>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {latency(venue.bookAgeP50)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {latency(venue.bookAgeP95)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {latency(venue.bookAgeP99)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {maximumBucket(
                                      worstLatency(
                                        metrics.pipeline.latencies,
                                        "arbitrage_orderbook_age_seconds",
                                        (summary) =>
                                          venueLabel(summary.labels.venue) ===
                                          venue.venue,
                                      ),
                                    )}
                                  </TableCell>
                                </>
                              ) : (
                                <>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {latency(venue.submitP50)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {latency(venue.submitP95)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {latency(venue.submitP99)}
                                  </TableCell>
                                  <TableCell className="numeric text-right font-mono text-xs">
                                    {maximumBucket(
                                      worstLatency(
                                        metrics.pipeline.latencies,
                                        "order_operation_seconds",
                                        (summary) =>
                                          venueLabel(summary.labels.venue) ===
                                            venue.venue &&
                                          summary.labels.operation === "submit",
                                      ),
                                    )}
                                  </TableCell>
                                </>
                              )}
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    )}
                  </CardContent>
                </Card>

                <Card className="xl:col-span-4">
                  <CardHeader>
                    <CardTitle>Top failure reasons</CardTitle>
                    <CardDescription>
                      Five most frequent terminal order outcomes.
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="px-2 sm:px-5">
                    {!metrics.pipeline.failures.length ? (
                      <p className="px-3 pb-4 text-xs text-muted-foreground">
                        No terminal failures have been recorded yet.
                      </p>
                    ) : (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Reason</TableHead>
                            <TableHead>Venue · leg</TableHead>
                            <TableHead className="text-right">Count</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {metrics.pipeline.failures.map((failure) => (
                            <TableRow key={failure.id}>
                              <TableCell className="max-w-40 truncate text-xs font-medium">
                                {failure.reason}
                              </TableCell>
                              <TableCell className="text-xs text-muted-foreground">
                                <span className="flex items-center gap-2">
                                  <VenueLogo venue={failure.venue} />
                                  {venueLabel(failure.venue)} · {failure.leg}
                                </span>
                              </TableCell>
                              <TableCell className="numeric text-right text-xs">
                                {failure.count}
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    )}
                  </CardContent>
                </Card>
              </section>
            </>
          )}
        </div>
      </details>
    </>
  )
}
