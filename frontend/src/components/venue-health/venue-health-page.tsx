import { Activity, Clock3, Gauge } from "lucide-react"

import { PageHeader } from "@/components/layout/page-header"
import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { VenueLogo } from "@/components/venues/venue-logo"
import { useVenueHealth } from "@/features/venue-health/use-venue-health"
import { formatTimestamp } from "@/lib/formatters"
import type { VenueHealthStatus } from "@/types/venue-health"

const statusView: Record<
  VenueHealthStatus,
  { label: string; badge: "positive" | "warning" | "negative"; dot: string }
> = {
  operational: {
    label: "Operational",
    badge: "positive",
    dot: "bg-emerald-400 shadow-[0_0_12px_rgb(52_211_153/0.65)]",
  },
  degraded: {
    label: "Degraded",
    badge: "warning",
    dot: "bg-amber-400 shadow-[0_0_12px_rgb(251_191_36/0.55)]",
  },
  unavailable: {
    label: "Unavailable",
    badge: "negative",
    dot: "bg-rose-400 shadow-[0_0_12px_rgb(251_113_133/0.55)]",
  },
}

/** Show independently cached availability checks for every trading venue. */
export function VenueHealthPage() {
  const { report, loading, error } = useVenueHealth()
  const overall = report ? statusView[report.overall_status] : null

  return (
    <div>
      <PageHeader
        eyebrow="Operations"
        title="Venue health"
        description="External venue availability checked outside the execution pipeline and cached to avoid adding load to trading or market-data streams."
        actions={
          overall ? (
            <Badge variant={overall.badge}>Overall · {overall.label}</Badge>
          ) : undefined
        }
      />

      {error && (
        <Card className="mb-4 border-rose-400/25">
          <CardContent className="p-4 text-sm text-rose-300">{error}</CardContent>
        </Card>
      )}

      {loading && !report ? (
        <Card>
          <CardContent className="flex items-center gap-3 p-5 text-sm text-muted-foreground">
            <Activity className="size-4 animate-pulse text-primary" />
            Checking venues…
          </CardContent>
        </Card>
      ) : (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {report?.venues.map((venue) => {
            const view = statusView[venue.status]
            return (
              <Card key={venue.venue_id}>
                <CardHeader className="flex-row items-start justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <VenueLogo venue={venue.venue_id} />
                    <div>
                      <CardTitle>{venue.venue_id}</CardTitle>
                      <CardDescription>{venue.source}</CardDescription>
                    </div>
                  </div>
                  <Badge variant={view.badge}>
                    <span className={`size-1.5 rounded-full ${view.dot}`} />
                    {view.label}
                  </Badge>
                </CardHeader>
                <CardContent className="space-y-4">
                  <p className="min-h-10 text-sm leading-relaxed text-muted-foreground">
                    {venue.message}
                  </p>
                  {venue.error_type && (
                    <div className="grid grid-cols-2 gap-3 rounded-lg border border-border/70 bg-background/35 p-3">
                      <div>
                        <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                          Error type
                        </p>
                        <p className="mt-1 break-all text-xs font-medium text-rose-300">
                          {venue.error_type}
                        </p>
                      </div>
                      <div>
                        <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                          Retry
                        </p>
                        <p className="mt-1 text-xs font-medium">
                          {venue.retryable ? "Yes" : "No"}
                        </p>
                      </div>
                    </div>
                  )}
                  <div className="grid grid-cols-3 gap-3 border-t border-border/70 pt-4">
                    <div>
                      <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                        HTTP response
                      </p>
                      <p className="numeric mt-1 text-sm font-medium">
                        {venue.http_status ?? "None"}
                      </p>
                    </div>
                    <div>
                      <p className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                        <Gauge className="size-3" /> Latency
                      </p>
                      <p className="numeric mt-1 text-sm font-medium">
                        {venue.latency_ms === null
                          ? "No response"
                          : `${Math.round(venue.latency_ms)} ms`}
                      </p>
                    </div>
                    <div>
                      <p className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
                        <Clock3 className="size-3" /> Checked
                      </p>
                      <p className="mt-1 text-xs font-medium">
                        {formatTimestamp(venue.checked_at)}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )
          })}
        </div>
      )}

      {report && (
        <p className="mt-4 text-right text-[11px] text-muted-foreground">
          Report generated {formatTimestamp(report.generated_at)} · refreshes every 60 seconds
        </p>
      )}
    </div>
  )
}
