import {
  Activity,
  ArrowRight,
  Database,
  Layers3,
  RadioTower,
  Send,
  Workflow,
} from "lucide-react"
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
import type { PipelineMetrics } from "@/types/metrics"
import type { RuntimeState } from "@/types/runtime"

function count(value: number | undefined | null) {
  return value == null ? "—" : value.toLocaleString()
}

/** Show current flow and queue state; absent telemetry stays explicitly unknown. */
export function PipelineOverview({
  runtime,
  pipeline,
  unavailable,
}: {
  runtime: RuntimeState | null
  pipeline: PipelineMetrics
  unavailable: boolean
}) {
  const events = runtime?.regular_markets?.length ?? 0
  const stages = [
    {
      label: "Market data",
      icon: RadioTower,
      value: count(runtime?.books),
      unit: "order books",
      description: "Live prices arrive from connected venues.",
    },
    {
      label: "Detection",
      icon: Workflow,
      value: count(runtime?.matched_pairs),
      unit: "matched pairs",
      description: "Compare complementary outcomes and costs.",
    },
    {
      label: "Dispatch",
      icon: Layers3,
      value: count(runtime?.output_buffer_size),
      unit: "queued outputs",
      description: "Prepare requests and check execution guards.",
    },
    {
      label: "Execution",
      icon: Send,
      value: count(runtime?.active_executions),
      unit: "active executions",
      description: "Submit both legs and follow their outcomes.",
    },
    {
      label: "Journal",
      icon: Database,
      value: count(runtime?.durable_sequence),
      unit: "durable sequence",
      description: "Persist execution events for recovery and reporting.",
    },
  ]
  const state =
    unavailable || !runtime
      ? "Unavailable"
      : runtime.safety_halted
        ? "Safety halted"
        : !runtime.running
          ? "Stopped"
          : events
            ? "Monitoring events"
            : "Waiting for events"
  const journalLag =
    runtime?.journal_sequence != null && runtime.durable_sequence != null
      ? Math.max(0, runtime.journal_sequence - runtime.durable_sequence)
      : null
  const projectionLag =
    runtime?.durable_sequence != null && runtime.projection_sequence != null
      ? Math.max(0, runtime.durable_sequence - runtime.projection_sequence)
      : null

  return (
    <div className="space-y-5">
      <section
        className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4"
        aria-label="Pipeline summary"
      >
        <StatusCard
          label="Runtime"
          value={state}
          detail="Current monitoring state"
          icon={Activity}
          tone={
            unavailable || runtime?.safety_halted
              ? "negative"
              : events
                ? "positive"
                : "neutral"
          }
        />
        <StatusCard
          label="Connected events"
          value={runtime ? count(events) : "—"}
          detail="Markets you have explicitly selected"
          icon={RadioTower}
        />
        <StatusCard
          label="Pending input"
          value={count(runtime?.input_buffer_size)}
          detail="Events waiting for the engine"
          icon={Layers3}
        />
        <StatusCard
          label="Trading"
          value={
            !runtime
              ? "Unknown"
              : runtime.trading_enabled
                ? "Enabled"
                : "Disabled"
          }
          detail="Separate from market observation"
          icon={Send}
          tone={runtime?.trading_enabled ? "warning" : "neutral"}
        />
      </section>

      <Card>
        <CardHeader className="sm:flex-row sm:items-start sm:justify-between">
          <div>
            <CardTitle>From market update to recorded execution</CardTitle>
            <CardDescription className="mt-2">
              Follow the flow from left to right. Each counter describes the
              current stage.
            </CardDescription>
          </div>
          <Badge variant="outline" className="w-fit shrink-0">
            5 stages
          </Badge>
        </CardHeader>
        <CardContent>
          <ol className="grid gap-3 xl:grid-cols-5">
            {stages.map(
              ({ label, icon: Icon, value, unit, description }, index) => (
                <li
                  key={label}
                  className="relative rounded-xl border border-border bg-background/60 p-4"
                >
                  <div className="mb-5 flex items-center justify-between">
                    <Icon className="size-5 text-primary" />
                    <span className="font-mono text-[11px] text-muted-foreground">
                      0{index + 1}
                    </span>
                  </div>
                  <p className="text-sm font-semibold">{label}</p>
                  <p className="numeric mt-4 text-3xl font-semibold tracking-tight">
                    {value}
                  </p>
                  <p className="mt-1 text-xs text-muted-foreground">{unit}</p>
                  <p className="mt-4 border-t border-border/70 pt-3 text-xs leading-relaxed text-muted-foreground">
                    {description}
                  </p>
                  {index < stages.length - 1 && (
                    <ArrowRight
                      aria-hidden="true"
                      className="absolute -right-3 top-7 z-10 hidden size-5 rounded-full bg-card text-muted-foreground xl:block"
                    />
                  )}
                </li>
              ),
            )}
          </ol>
          {runtime && !events && !unavailable && (
            <div className="mt-5 flex flex-col justify-between gap-4 rounded-xl bg-primary/5 p-4 sm:flex-row sm:items-center">
              <div>
                <p className="text-sm font-medium">
                  Your pipeline is waiting for its first event
                </p>
                <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                  Choose a matching market in the catalog to start receiving
                  live order books.
                </p>
              </div>
              <Button asChild size="sm">
                <a href="/events">
                  Explore events <ArrowRight className="size-3.5" />
                </a>
              </Button>
            </div>
          )}
        </CardContent>
      </Card>

      <section className="grid gap-5 xl:grid-cols-[1.4fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitle>Venue feeds</CardTitle>
            <CardDescription>
              Recent feed measurements. No samples does not mean a connection
              failure.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {["Polymarket", "Predict", "Limitless"].map((venue) => {
              const feed = pipeline.marketFeeds.find(
                (item) => item.venue.toLowerCase() === venue.toLowerCase(),
              )
              const socket =
                venue === "Polymarket" ? pipeline.polymarketWs : null
              const queueDepth =
                feed?.wsQueueDepth ?? socket?.queueDepth ?? null
              const paused = feed?.wsPaused || (socket?.pausedSockets ?? 0) > 0
              const hasSamples = [
                feed?.receiveToSinkP95,
                feed?.venueAgeP95,
                queueDepth,
                socket?.activeSockets,
              ].some((value) => value != null)
              return (
                <div
                  key={venue}
                  className="flex flex-wrap items-center justify-between gap-3 rounded-xl border border-border/70 p-4"
                >
                  <div className="flex items-center gap-3">
                    <VenueLogo venue={venue} className="size-8" />
                    <div>
                      <p className="text-sm font-medium">{venue}</p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {queueDepth == null
                          ? "Queue not reported"
                          : `${queueDepth} queued updates`}
                      </p>
                    </div>
                  </div>
                  <Badge
                    variant={
                      unavailable
                        ? "warning"
                        : paused
                          ? "negative"
                          : hasSamples
                            ? "positive"
                            : "outline"
                    }
                  >
                    {unavailable
                      ? "Telemetry unavailable"
                      : paused
                        ? "Paused"
                        : hasSamples
                          ? "Telemetry available"
                          : "No samples"}
                  </Badge>
                </div>
              )
            })}
            <a
              href="/latency"
              className="inline-flex items-center gap-2 pt-2 text-xs font-medium text-primary"
            >
              Compare feed latency <ArrowRight className="size-3.5" />
            </a>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Queues & persistence</CardTitle>
            <CardDescription>
              Pending work at each handoff. Lower counts mean less work waiting.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <dl className="divide-y divide-border/70">
              {[
                [
                  "Engine input",
                  runtime?.input_buffer_size,
                  "Market updates waiting to be processed",
                ],
                [
                  "Dispatcher output",
                  runtime?.output_buffer_size,
                  "Outputs waiting to be dispatched",
                ],
                [
                  "Awaiting durable write",
                  journalLag,
                  "Journal events not yet persisted",
                ],
                [
                  "Awaiting projection",
                  projectionLag,
                  "Durable events not yet reflected in the database",
                ],
                [
                  "Dropped order books",
                  runtime?.dropped_order_books,
                  "Cumulative drops reported by the runtime",
                ],
              ].map(([label, value, description]) => (
                <div
                  key={label}
                  className="flex items-center justify-between gap-4 py-3.5 first:pt-0 last:pb-0"
                >
                  <div>
                    <dt className="text-sm">{label}</dt>
                    <p className="mt-1 text-[11px] text-muted-foreground">
                      {description}
                    </p>
                  </div>
                  <dd className="numeric text-xl font-semibold">
                    {count(typeof value === "number" ? value : null)}
                  </dd>
                </div>
              ))}
            </dl>
          </CardContent>
        </Card>
      </section>
    </div>
  )
}
