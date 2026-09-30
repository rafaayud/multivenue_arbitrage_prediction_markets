import { useState, type PointerEvent } from "react"
import {
  ChartNoAxesCombined,
  Clock3,
  Fuel,
  ReceiptText,
  WalletCards,
} from "lucide-react"

import { ExecutionFeedStatus } from "@/components/execution/execution-status"
import { PageHeader } from "@/components/layout/page-header"
import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { VenueLogo } from "@/components/venues/venue-logo"
import { usePnl } from "@/features/pnl/use-pnl"
import { formatPrice, formatTimestamp } from "@/lib/formatters"
import type {
  LedgerPosition,
  PerformanceView,
  PnlPoint,
  PnlView,
  TimeRange,
} from "@/types/pnl"

function number(value: string | null): number | null {
  if (value === null) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function formatUsd(value: string | null): string {
  const parsed = number(value)
  if (parsed === null) return "N/A"
  return `${parsed < 0 ? "-" : ""}$${formatPrice(Math.abs(parsed))}`
}

function tone(value: string | null): string {
  const parsed = number(value)
  if (parsed === null) return "text-muted-foreground"
  return parsed >= 0 ? "text-emerald-300" : "text-rose-400"
}

function label(value: string | null): string {
  return value ? value.replaceAll("_", " ") : "Unknown"
}

function positionUnrealized(position: LedgerPosition): string | null {
  const average = number(position.average_entry_price)
  const current = number(position.current_price)
  const quantity = number(position.quantity)
  if (average === null || current === null || quantity === null) return null
  return String(
    quantity * (position.side === "short" ? average - current : current - average),
  )
}

function PerformanceChart({ series }: { series: PnlPoint[] }) {
  const [hovered, setHovered] = useState<number | null>(null)
  if (!series.length) {
    return (
      <div className="flex h-72 items-center justify-center rounded-xl border border-dashed border-border text-sm text-muted-foreground">
        No hay una serie comparable para esta vista y horizonte.
      </div>
    )
  }

  const width = 960
  const height = 300
  const padding = 24
  const values = series.map((point) => Number(point.net_pnl_usd))
  const minimum = Math.min(0, ...values)
  const maximum = Math.max(0, ...values)
  const spread = maximum - minimum || 1
  const x = (index: number) =>
    values.length === 1
      ? width / 2
      : padding + (index / (values.length - 1)) * (width - padding * 2)
  const y = (value: number) =>
    padding + ((maximum - value) / spread) * (height - padding * 2)
  const line = values.map((value, index) => `${x(index)},${y(value)}`).join(" ")
  const area = `${padding},${height - padding} ${line} ${width - padding},${height - padding}`
  const active = hovered === null ? values.length - 1 : hovered
  const positive = values.at(-1)! >= 0

  function move(event: PointerEvent<SVGSVGElement>) {
    const bounds = event.currentTarget.getBoundingClientRect()
    const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width))
    setHovered(Math.round(ratio * (values.length - 1)))
  }

  return (
    <div className="relative pt-8">
      <div
        className="pointer-events-none absolute top-0 rounded-lg border border-border bg-popover px-3 py-2 text-xs shadow-xl"
        style={{
          left: `${(x(active) / width) * 100}%`,
          transform: "translateX(-50%)",
        }}
      >
        <p className="font-semibold">{formatUsd(series[active]!.net_pnl_usd)}</p>
        <p className="text-[10px] text-muted-foreground">
          {formatTimestamp(series[active]!.observed_at)}
        </p>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className={`h-72 w-full touch-none ${positive ? "text-primary" : "text-rose-400"}`}
        role="img"
        aria-label="Portfolio performance in USD"
        onPointerMove={move}
        onPointerLeave={() => setHovered(null)}
      >
        <title>Portfolio performance in USD</title>
        <defs>
          <linearGradient id="pnl-area" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="currentColor" stopOpacity="0.22" />
            <stop offset="1" stopColor="currentColor" stopOpacity="0" />
          </linearGradient>
        </defs>
        {[0, 0.25, 0.5, 0.75, 1].map((ratio) => (
          <line
            key={ratio}
            x1={padding}
            x2={width - padding}
            y1={padding + ratio * (height - padding * 2)}
            y2={padding + ratio * (height - padding * 2)}
            className="stroke-border"
            vectorEffect="non-scaling-stroke"
          />
        ))}
        <line
          x1={padding}
          x2={width - padding}
          y1={y(0)}
          y2={y(0)}
          className="stroke-muted-foreground/60"
          strokeDasharray="5 5"
          vectorEffect="non-scaling-stroke"
        />
        <polygon points={area} fill="url(#pnl-area)" />
        <polyline
          points={line}
          fill="none"
          stroke="currentColor"
          strokeWidth="2.5"
          strokeLinejoin="round"
          strokeLinecap="round"
          vectorEffect="non-scaling-stroke"
        />
        <line
          x1={x(active)}
          x2={x(active)}
          y1={padding}
          y2={height - padding}
          className="stroke-muted-foreground/50"
          strokeDasharray="3 3"
          vectorEffect="non-scaling-stroke"
        />
        <circle cx={x(active)} cy={y(values[active]!)} r="5" fill="currentColor" />
      </svg>
      <div className="flex justify-between text-[11px] text-muted-foreground">
        <span>{formatTimestamp(series[0]!.observed_at)}</span>
        <span>{formatTimestamp(series.at(-1)!.observed_at)}</span>
      </div>
    </div>
  )
}

function qualityBadges(view: PerformanceView, stale: boolean) {
  const values = [
    ...(view.partial ? ["Partial"] : []),
    ...(!view.comparability.comparable ? ["Inconsistent scope"] : []),
    ...(view.quality_flags.some((flag) => flag.includes("FEE"))
      ? ["Missing fees"]
      : []),
    ...(stale ? ["Stale"] : []),
  ]
  return [...new Set(values)]
}

/** Portfolio-first PnL dashboard with diagnostics kept below the main view. */
export function PnlPage() {
  const [selectedView, setSelectedView] = useState<PnlView>("bot_ledger")
  const [selectedRange, setSelectedRange] = useState<TimeRange>("1M")
  const { pnl, loading, error, updatedAt } = usePnl(selectedView, selectedRange)
  const performance = pnl?.portfolio_performance
  const active = performance?.[selectedView] ?? null
  const stale = pnl?.venue_health.some((venue) => venue.stale) ?? false
  const badges = active ? qualityBadges(active, stale) : []
  const stats = [
    { name: "Realized", value: active?.summary.realized ?? null, Icon: WalletCards },
    { name: "Unrealized", value: active?.summary.unrealized ?? null, Icon: ChartNoAxesCombined },
    { name: "Trading fees", value: active?.summary.fees ?? null, Icon: ReceiptText },
    { name: "Gas", value: active?.summary.gas ?? null, Icon: Fuel },
  ]

  function chooseView(view: PnlView) {
    setSelectedView(view)
    if (view === "venue_account" && selectedRange !== "ALL") setSelectedRange("ALL")
  }

  return (
    <>
      <PageHeader
        eyebrow="Portfolio"
        title="Performance"
        description="PnL contable derivado de trades y operaciones económicas; los datos de venues quedan como diagnóstico."
      />
      <ExecutionFeedStatus />

      {error && (
        <div className="mb-4 rounded-lg border border-rose-400/25 bg-rose-400/10 px-4 py-3 text-xs text-rose-300">
          {error}
        </div>
      )}

      <Card className="mb-4 overflow-hidden border-primary/15 bg-gradient-to-br from-primary/[0.07] via-card to-card">
        <CardContent className="p-5 sm:p-7">
          <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
            <div>
              <div className="mb-4 flex flex-wrap items-center gap-2">
                <button
                  type="button"
                  aria-pressed={selectedView === "bot_ledger"}
                  onClick={() => chooseView("bot_ledger")}
                  className={`rounded-full px-3 py-1.5 text-xs font-medium transition ${
                    selectedView === "bot_ledger"
                      ? "bg-primary text-primary-foreground"
                      : "bg-secondary text-muted-foreground hover:text-foreground"
                  }`}
                >
                  Bot ledger
                </button>
                <button
                  type="button"
                  aria-pressed={selectedView === "venue_account"}
                  onClick={() => chooseView("venue_account")}
                  className={`rounded-full px-3 py-1.5 text-xs font-medium transition ${
                    selectedView === "venue_account"
                      ? "bg-primary text-primary-foreground"
                      : "bg-secondary text-muted-foreground hover:text-foreground"
                  }`}
                >
                  Venue account
                </button>
              </div>
              <p className="text-xs font-medium uppercase tracking-[0.16em] text-muted-foreground">
                Total PnL
              </p>
              <p className={`numeric mt-2 text-4xl font-semibold sm:text-5xl ${tone(active?.summary.total ?? null)}`}>
                {loading && !pnl ? "..." : formatUsd(active?.summary.total ?? null)}
              </p>
              <div className="mt-3 flex flex-wrap items-center gap-2">
                {badges.map((value) => (
                  <Badge key={value} variant="warning">{value}</Badge>
                ))}
                {!badges.length && active && <Badge variant="positive">Complete</Badge>}
              </div>
            </div>
            <div className="grid min-w-0 gap-3 sm:grid-cols-2 xl:min-w-[720px] xl:grid-cols-4">
              {stats.map(({ name, value, Icon }) => (
                <div key={name} className="rounded-xl border border-border/80 bg-background/55 p-4 backdrop-blur">
                  <div className="flex items-center justify-between text-xs text-muted-foreground">
                    <span>{name}</span>
                    <Icon className="size-4" />
                  </div>
                  <p className={`numeric mt-2 text-xl font-semibold ${name === "Trading fees" || name === "Gas" ? "text-amber-300" : tone(value)}`}>
                    {formatUsd(value)}
                  </p>
                </div>
              ))}
            </div>
          </div>
          <div className="mt-6 flex flex-wrap items-center justify-between gap-3 border-t border-border/70 pt-4">
            <div className="flex flex-wrap gap-1" aria-label="Performance range">
              {(performance?.available_ranges ?? ["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"]).map((range) => {
                const disabled = selectedView === "venue_account" && range !== "ALL"
                return (
                  <button
                    key={range}
                    type="button"
                    disabled={disabled}
                    aria-pressed={selectedRange === range}
                    title={disabled ? "Venue APIs do not expose a common historical range" : undefined}
                    onClick={() => setSelectedRange(range)}
                    className={`rounded-md px-2.5 py-1.5 text-xs transition ${
                      selectedRange === range
                        ? "bg-secondary text-foreground"
                        : "text-muted-foreground hover:text-foreground"
                    } disabled:cursor-not-allowed disabled:opacity-30`}
                  >
                    {range}
                  </button>
                )
              })}
            </div>
            <p className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
              <Clock3 className="size-3.5" />
              Updated {active ? formatTimestamp(active.last_updated) : "—"}
            </p>
          </div>
        </CardContent>
      </Card>

      <Card className="mb-4 border-emerald-400/15">
        <CardHeader>
          <CardTitle>Closed execution result</CardTitle>
          <CardDescription>
            End-to-end result of terminal arbitrages: every initial fill,
            failed leg and automatic recovery, with trading fees included.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 sm:grid-cols-4">
          <div>
            <p className="text-xs text-muted-foreground">Net earned</p>
            <p
              className={`numeric mt-1 text-xl font-semibold ${tone(
                pnl?.terminal_executions.net_pnl_usd ?? null,
              )}`}
            >
              {formatUsd(pnl?.terminal_executions.net_pnl_usd ?? null)}
            </p>
          </div>
          <div>
            <p className="text-xs text-muted-foreground">Gross result</p>
            <p className="numeric mt-1 text-xl font-semibold">
              {formatUsd(pnl?.terminal_executions.gross_pnl_usd ?? null)}
            </p>
          </div>
          <div>
            <p className="text-xs text-muted-foreground">Trading fees</p>
            <p className="numeric mt-1 text-xl font-semibold text-amber-300">
              {formatUsd(pnl?.terminal_executions.fees_usd ?? null)}
            </p>
          </div>
          <div>
            <p className="text-xs text-muted-foreground">Priced executions</p>
            <p className="numeric mt-1 text-xl font-semibold">
              {pnl?.terminal_executions.priced_terminal_executions ?? "—"}
            </p>
          </div>
        </CardContent>
      </Card>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Portfolio performance</CardTitle>
          <CardDescription>
            {active?.methodology ?? "Waiting for the persisted accounting series."}
            {active?.methodology_note ? ` ${active.methodology_note}` : ""}
          </CardDescription>
        </CardHeader>
        <CardContent>
          <PerformanceChart series={active?.series ?? []} />
        </CardContent>
      </Card>

      <details className="group mb-4 rounded-xl border border-border bg-card">
        <summary className="flex cursor-pointer list-none items-center justify-between p-5">
          <div>
            <p className="font-semibold">Reconciliation</p>
            <p className="mt-1 text-xs text-muted-foreground">
              Secondary diagnostic: ledger versus venue-reported account values.
            </p>
          </div>
          <Badge variant="outline">
            Difference {formatUsd(pnl?.reconciliation.difference_usd ?? null)}
          </Badge>
        </summary>
        <div className="grid gap-3 border-t border-border p-5 sm:grid-cols-3">
          <div><p className="text-xs text-muted-foreground">Bot ledger</p><p className="numeric mt-1 text-lg">{formatUsd(pnl?.reconciliation.internal_net_pnl_usd ?? null)}</p></div>
          <div><p className="text-xs text-muted-foreground">Venue reported</p><p className="numeric mt-1 text-lg">{formatUsd(pnl?.reconciliation.venue_reported_net_pnl_usd ?? null)}</p></div>
          <div><p className="text-xs text-muted-foreground">Difference</p><p className={`numeric mt-1 text-lg ${tone(pnl?.reconciliation.difference_usd ?? null)}`}>{formatUsd(pnl?.reconciliation.difference_usd ?? null)}</p></div>
          {!!pnl?.reconciliation.notes.length && (
            <ul className="col-span-full list-disc space-y-1 pl-4 text-xs text-muted-foreground">
              {pnl.reconciliation.notes.map((note) => <li key={note}>{note}</li>)}
            </ul>
          )}
        </div>
      </details>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Venue health & data quality</CardTitle>
          <CardDescription>Freshness, scope and fee coverage stay visible without contaminating the main KPI.</CardDescription>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <Table>
            <TableHeader><TableRow><TableHead>Venue</TableHead><TableHead>Status</TableHead><TableHead>Scope</TableHead><TableHead>Fees</TableHead><TableHead>Observed</TableHead></TableRow></TableHeader>
            <TableBody>
              {!pnl?.venue_health.length ? (
                <TableRow><TableCell colSpan={5} className="h-24 text-center text-xs text-muted-foreground">No venue diagnostics available.</TableCell></TableRow>
              ) : pnl.venue_health.map((venue) => (
                <TableRow key={venue.venue_id}>
                  <TableCell><div className="flex items-center gap-2 font-medium"><VenueLogo venue={venue.venue_id} />{venue.venue_id}</div></TableCell>
                  <TableCell><Badge variant={venue.stale ? "warning" : venue.status === "operational" ? "positive" : "negative"}>{venue.stale ? "stale" : venue.status}</Badge></TableCell>
                  <TableCell className="max-w-64 text-xs capitalize text-muted-foreground">{label(venue.scope)}</TableCell>
                  <TableCell><Badge variant={venue.missing_fees ? "warning" : "positive"}>{venue.missing_fees ? "missing" : "included"}</Badge></TableCell>
                  <TableCell className="text-xs text-muted-foreground">{venue.observed_at ? formatTimestamp(venue.observed_at) : "N/A"}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-start justify-between">
          <div><CardTitle>Ledger positions</CardTitle><CardDescription>Current WAC positions derived from fills and confirmed inventory operations.</CardDescription></div>
          <Badge variant="outline">{pnl?.positions.length ?? 0} positions</Badge>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <Table>
            <TableHeader><TableRow><TableHead>Venue / contract</TableHead><TableHead>Side</TableHead><TableHead className="text-right">Quantity</TableHead><TableHead className="text-right">Average</TableHead><TableHead className="text-right">Mark</TableHead><TableHead className="text-right">Realized</TableHead><TableHead className="text-right">Unrealized</TableHead><TableHead className="text-right">Fees</TableHead></TableRow></TableHeader>
            <TableBody>
              {!pnl?.positions.length ? (
                <TableRow><TableCell colSpan={8} className="h-24 text-center text-xs text-muted-foreground">{loading ? "Loading positions…" : "No ledger position recorded yet."}</TableCell></TableRow>
              ) : pnl.positions.map((position) => {
                const unrealized = positionUnrealized(position)
                return (
                  <TableRow key={position.position_id}>
                    <TableCell className="max-w-72"><div className="flex items-center gap-2 font-medium"><VenueLogo venue={position.venue_id} />{position.venue_id}</div><p className="truncate text-[10px] text-muted-foreground" title={position.contract_id}>{position.contract_id}</p></TableCell>
                    <TableCell><Badge variant={position.side === "flat" ? "outline" : position.side === "long" ? "positive" : "warning"}>{position.side}</Badge></TableCell>
                    <TableCell className="numeric text-right">{formatPrice(Number(position.quantity))}</TableCell>
                    <TableCell className="numeric text-right">{position.average_entry_price ?? "N/A"}</TableCell>
                    <TableCell className="numeric text-right">{position.current_price ?? "N/A"}</TableCell>
                    <TableCell className={`numeric text-right ${tone(position.realized_pnl)}`}>{formatUsd(position.realized_pnl)}</TableCell>
                    <TableCell className={`numeric text-right ${tone(unrealized)}`}>{formatUsd(unrealized)}</TableCell>
                    <TableCell className="numeric text-right text-amber-300">{formatUsd(position.fee_settlement_amount)}</TableCell>
                  </TableRow>
                )
              })}
            </TableBody>
          </Table>
          <p className="mt-3 px-2 text-[11px] text-muted-foreground">
            {updatedAt ? `Dashboard refreshed ${updatedAt.toLocaleTimeString()}.` : ""}
          </p>
        </CardContent>
      </Card>
    </>
  )
}
