import { Fragment, type FormEvent, useState } from "react"
import {
  Activity,
  Fuel,
  ListChecks,
  LoaderCircle,
  RefreshCcw,
  ReceiptText,
} from "lucide-react"
import { toast } from "sonner"

import {
  ExecutionFeedStatus,
  ExecutionStateBadge,
} from "@/components/execution/execution-status"
import { PageHeader } from "@/components/layout/page-header"
import { Badge } from "@/components/ui/badge"
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Button } from "@/components/ui/button"
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
import { useExecutionActivity } from "@/features/execution/execution-activity-provider"
import { useRuntime } from "@/features/runtime/use-runtime"
import { apiClient } from "@/lib/api-client"
import {
  intervalLabel,
  formatPrice,
  formatPercent,
  formatQuantity,
  formatTimestamp,
} from "@/lib/formatters"
import type {
  ExecutionJournal,
  ExecutionLeg,
  ExecutionOrder,
  ExposureRecovery,
  ManualExecutionResolution,
} from "@/types/execution"

function deployedCapital(journal: ExecutionJournal): number | null {
  if (
    journal.netLockedPnlUsd === null ||
    journal.leg1.filledQuantity <= 0 ||
    journal.leg2.filledQuantity <= 0
  ) {
    return null
  }

  return [journal.leg1, journal.leg2].reduce(
    (total, leg) =>
      total + leg.filledQuantity * (leg.averageFillPrice ?? leg.limitPrice),
    0,
  )
}

function recoveryFees(recovery: ExposureRecovery): number | null {
  const recoveryFee =
    recovery.recoveryFeeAmount ?? recovery.estimatedRecoveryFeeAmount
  if (recovery.sourceFeeAmount === null || recoveryFee === null) {
    return null
  }
  return recovery.sourceFeeAmount + recoveryFee
}

function signedUsd(value: number | null): string {
  if (value === null) return "Pending"
  const amount = `$${formatPrice(Math.abs(value))}`
  return value > 0 ? `+${amount}` : value < 0 ? `-${amount}` : amount
}

/** Explain how one abnormal two-leg execution was neutralized. */
function RecoveryDetails({
  journal,
  recovery,
  orders,
}: {
  journal: ExecutionJournal
  recovery: ExposureRecovery | undefined
  orders: ExecutionOrder[]
}) {
  const legs: Array<{ label: string; leg: ExecutionLeg }> = [
    { label: "Primary", leg: journal.leg1 },
    { label: "Hedge", leg: journal.leg2 },
  ]
  const failed = legs.find(({ leg }) => leg.filledQuantity < leg.quantity)
  const source =
    legs.find(({ leg }) => leg.contractId === recovery?.sourceContractId) ??
    legs.find(({ leg }) => leg.filledQuantity > 0)
  const failedOrder = orders.find(
    (order) => order.clientOrderId === failed?.leg.clientOrderId,
  )
  const fees = recovery ? recoveryFees(recovery) : null
  const durationSeconds = recovery
    ? Math.max(
        0,
        (new Date(recovery.updatedAt).getTime() -
          new Date(journal.createdAt).getTime()) /
          1_000,
      )
    : null

  return (
    <div className="rounded-lg border border-amber-400/20 bg-amber-400/[0.04] p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="warning">Recovered after a failed leg</Badge>
            <span className="text-xs text-muted-foreground">
              Automatic recovery neutralized the unmatched fill; this result
              excludes quantity already paired.
            </span>
          </div>
          <p className="mt-2 text-xs text-rose-300">
            {journal.lastError ??
              (failed
                ? `${failed.label} order ${failedOrder?.status.replaceAll("_", " ") ?? "did not fill"}.`
                : "The original legs did not finish together.")}
          </p>
        </div>
        <div className="text-right">
          <p className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
            Residual recovery result
          </p>
          <p
            className={`numeric mt-1 font-mono text-lg font-semibold ${
              recovery?.actualNetResult == null
                ? "text-muted-foreground"
                : recovery.actualNetResult >= 0
                  ? "text-emerald-300"
                  : "text-rose-300"
            }`}
          >
            {signedUsd(recovery?.actualNetResult ?? null)} net
          </p>
        </div>
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-3">
        <div className="rounded-md border border-border/60 bg-background/40 p-3">
          <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
            1 · {failed?.label ?? "Original leg"} failed
          </p>
          <p className="mt-2 text-sm font-semibold">
            {failed?.leg.venueId ?? "Unknown venue"} · {failed?.leg.side ?? "—"}
          </p>
          <p className="numeric mt-1 font-mono text-xs text-muted-foreground">
            {failed
              ? `${formatQuantity(failed.leg.filledQuantity)} / ${formatQuantity(failed.leg.quantity)} @ ${formatPrice(failed.leg.limitPrice)}`
              : "No fill details"}
          </p>
          <p className="mt-2 text-xs capitalize text-rose-300">
            {failedOrder?.status.replaceAll("_", " ") ?? "No fill"}
          </p>
        </div>

        <div className="rounded-md border border-border/60 bg-background/40 p-3">
          <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
            2 · {source?.label ?? "Other leg"} filled
          </p>
          <p className="mt-2 text-sm font-semibold">
            {source?.leg.venueId ?? "Unknown venue"} · {source?.leg.side ?? "—"}
          </p>
          <p className="numeric mt-1 font-mono text-xs text-muted-foreground">
            {source
              ? `${formatQuantity(source.leg.filledQuantity)} / ${formatQuantity(source.leg.quantity)} @ ${formatPrice(source.leg.averageFillPrice ?? source.leg.limitPrice)}`
              : "No fill details"}
          </p>
          <p className="numeric mt-2 font-mono text-xs text-muted-foreground">
            Fee: {recovery?.sourceFeeAmount == null
              ? "pending"
              : `${formatPrice(recovery.sourceFeeAmount)} ${recovery.sourceFeeCurrency ?? ""}`}
          </p>
        </div>

        <div className="rounded-md border border-border/60 bg-background/40 p-3">
          <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
            3 · Automatic recovery
          </p>
          <p className="mt-2 text-sm font-semibold">
            {recovery?.venueId ?? "Pending"} · {recovery?.side ?? "—"}
          </p>
          <p className="numeric mt-1 font-mono text-xs text-muted-foreground">
            {recovery
              ? `${formatQuantity(recovery.filledQuantity)} / ${formatQuantity(recovery.quantity)} @ ${formatPrice(recovery.averagePrice ?? recovery.limitPrice)}`
              : "Recovery record unavailable"}
          </p>
          <p className="numeric mt-2 font-mono text-xs text-muted-foreground">
            Fee: {recovery?.recoveryFeeAmount == null
              ? "pending"
              : `${formatPrice(recovery.recoveryFeeAmount)} ${recovery.recoveryFeeCurrency ?? ""}`}
          </p>
        </div>
      </div>

      <div className="mt-3 grid gap-3 rounded-md border border-border/60 bg-background/40 p-3 sm:grid-cols-2 xl:grid-cols-5">
        {[
          ["Outcome", journal.residualQuantity === 0 ? "Exposure neutralized" : "Exposure remains"],
          ["Residual", formatQuantity(journal.residualQuantity)],
          ["Estimated net", signedUsd(recovery?.estimatedNetResult ?? null)],
          ["Gross result", signedUsd(recovery?.actualGrossResult ?? null)],
          ["Residual segment fees", fees === null ? "Pending" : `$${formatPrice(fees)}`],
        ].map(([label, value]) => (
          <div key={label}>
            <p className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">
              {label}
            </p>
            <p className="numeric mt-1 font-mono text-xs font-semibold">{value}</p>
          </div>
        ))}
      </div>

      <details className="mt-3 text-xs text-muted-foreground">
        <summary className="cursor-pointer font-semibold text-foreground">
          Technical details
        </summary>
        <dl className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-3">
          <div>
            <dt>Execution ID</dt>
            <dd className="mt-1 break-all font-mono text-[11px] text-foreground">
              {journal.executionId}
            </dd>
          </div>
          <div>
            <dt>Primary order</dt>
            <dd className="mt-1 break-all font-mono text-[11px] text-foreground">
              {journal.leg1.orderId ?? "Pending"}
            </dd>
          </div>
          <div>
            <dt>Hedge order</dt>
            <dd className="mt-1 break-all font-mono text-[11px] text-foreground">
              {journal.leg2.orderId ?? "Pending"}
            </dd>
          </div>
          <div>
            <dt>Recovery order</dt>
            <dd className="mt-1 break-all font-mono text-[11px] text-foreground">
              {recovery?.orderId ?? "Pending"}
            </dd>
          </div>
          <div>
            <dt>Route</dt>
            <dd className="mt-1 capitalize text-foreground">
              {recovery?.route?.replaceAll("_", " ") ?? "Pending"}
            </dd>
          </div>
          <div>
            <dt>Duration</dt>
            <dd className="mt-1 text-foreground">
              {durationSeconds === null ? "Pending" : `${durationSeconds.toFixed(2)}s`}
            </dd>
          </div>
        </dl>
      </details>
    </div>
  )
}

function residualSource(
  journal: ExecutionJournal,
  recovery: ExposureRecovery | undefined,
): ExecutionLeg {
  if (recovery?.sourceContractId === journal.leg1.contractId) {
    return journal.leg1
  }
  if (recovery?.sourceContractId === journal.leg2.contractId) {
    return journal.leg2
  }
  return journal.leg1.filledQuantity > journal.leg2.filledQuantity
    ? journal.leg1
    : journal.leg2
}

function localDateTimeNow(): string {
  const now = new Date()
  return new Date(now.getTime() - now.getTimezoneOffset() * 60_000)
    .toISOString()
    .slice(0, 16)
}

/** Collect actual external economics while keeping execution identity read-only. */
function ManualResolutionDialog({
  journal,
  recovery,
  tradingEnabled,
}: {
  journal: ExecutionJournal
  recovery: ExposureRecovery | undefined
  tradingEnabled: boolean
}) {
  const source = residualSource(journal, recovery)
  const [open, setOpen] = useState(false)
  const [method, setMethod] =
    useState<ManualExecutionResolution["method"]>("settlement")
  const [price, setPrice] = useState("1")
  const [fee, setFee] = useState("0")
  const [executedAt, setExecutedAt] = useState(localDateTimeNow)
  const [reference, setReference] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setBusy(true)
    setError(null)
    try {
      await apiClient.completeExecution(journal.executionId, {
        method,
        price: Number(price),
        feeAmountUsd: Number(fee),
        executedAt: new Date(executedAt).toISOString(),
        externalReference: reference.trim() || null,
      })
      setOpen(false)
      toast.success("Manual resolution recorded")
    } catch (reason) {
      setError(
        reason instanceof Error ? reason.message : "Manual resolution failed",
      )
    } finally {
      setBusy(false)
    }
  }

  return (
    <AlertDialog open={open} onOpenChange={setOpen}>
      <AlertDialogTrigger asChild>
        <Button size="sm" disabled={tradingEnabled}>
          Resolve manually
        </Button>
      </AlertDialogTrigger>
      <AlertDialogContent className="max-h-[calc(100vh-2rem)] overflow-y-auto">
        <form onSubmit={(event) => void submit(event)}>
          <AlertDialogHeader>
            <AlertDialogTitle>Record manual resolution</AlertDialogTitle>
            <AlertDialogDescription>
              This records accounting only. It will not send an order to {source.venueId}.
            </AlertDialogDescription>
          </AlertDialogHeader>

          <div className="mt-4 rounded-md border border-border bg-background/50 p-3 text-xs">
            <p>
              Close <span className="font-semibold">{formatQuantity(journal.residualQuantity)}</span>{" "}
              contracts on <span className="font-semibold">{source.venueId}</span>.
            </p>
            <p className="mt-1 break-all font-mono text-[11px] text-muted-foreground">
              {source.contractId}
            </p>
          </div>

          <div className="mt-4 grid gap-4 sm:grid-cols-2">
            <label className="block text-xs font-medium">
              Resolution method
              <select
                value={method}
                onChange={(event) => {
                  const next = event.target.value as ManualExecutionResolution["method"]
                  setMethod(next)
                  if (next === "settlement") setPrice("1")
                }}
                className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <option value="settlement">Settlement / claim</option>
                <option value="manual_sale">Manual sale</option>
              </select>
            </label>
            <label className="block text-xs font-medium">
              {method === "settlement" ? "Payout per contract" : "Exit price"}
              <input
                required
                type="number"
                min="0"
                max="1"
                step="any"
                value={price}
                onChange={(event) => setPrice(event.target.value)}
                className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 font-mono text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </label>
            <label className="block text-xs font-medium">
              Fee (USD)
              <input
                required
                type="number"
                min="0"
                step="any"
                value={fee}
                onChange={(event) => setFee(event.target.value)}
                className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 font-mono text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </label>
            <label className="block text-xs font-medium">
              Resolved at
              <input
                required
                type="datetime-local"
                value={executedAt}
                onChange={(event) => setExecutedAt(event.target.value)}
                className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              />
            </label>
          </div>
          <label className="mt-4 block text-xs font-medium">
            Transaction, claim, or note (optional)
            <input
              maxLength={256}
              value={reference}
              onChange={(event) => setReference(event.target.value)}
              className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
              placeholder="Venue transaction ID or operator note"
            />
          </label>

          {error ? (
            <p role="alert" className="mt-4 text-xs text-rose-300">
              {error}
            </p>
          ) : null}

          <AlertDialogFooter>
            <AlertDialogCancel type="button">Cancel</AlertDialogCancel>
            <Button type="submit" disabled={busy}>
              {busy ? <LoaderCircle className="size-4 animate-spin" /> : null}
              Record resolution
            </Button>
          </AlertDialogFooter>
        </form>
      </AlertDialogContent>
    </AlertDialog>
  )
}

/** Explain the operator-entered trade used to close residual exposure. */
function ManualResolutionDetails({
  journal,
  resolution,
}: {
  journal: ExecutionJournal
  resolution: ManualExecutionResolution
}) {
  return (
    <div className="rounded-lg border border-emerald-400/20 bg-emerald-400/[0.04] p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <Badge variant="positive">Manually resolved</Badge>
          <p className="mt-2 text-xs text-muted-foreground">
            {resolution.method === "settlement"
              ? "Held to settlement / claim"
              : "Closed with an external sale"}
            {resolution.externalReference ? ` · ${resolution.externalReference}` : ""}
          </p>
        </div>
        <p className="numeric font-mono text-lg font-semibold text-emerald-300">
          {signedUsd(journal.netLockedPnlUsd)} net
        </p>
      </div>
      <div className="mt-3 grid gap-3 text-xs sm:grid-cols-2 lg:grid-cols-5">
        {[
          ["Venue", resolution.venueId],
          ["Accounting action", resolution.side.toUpperCase()],
          ["Quantity", formatQuantity(resolution.quantity)],
          ["Final price", formatPrice(resolution.price)],
          ["Fee", `$${formatPrice(resolution.feeAmountUsd)}`],
        ].map(([label, value]) => (
          <div key={label}>
            <p className="text-[10px] uppercase tracking-[0.12em] text-muted-foreground">{label}</p>
            <p className="numeric mt-1 break-all font-mono font-semibold">{value}</p>
          </div>
        ))}
      </div>
    </div>
  )
}

function SideBadge({ side }: { side: string }) {
  return (
    <Badge variant={side.toLowerCase() === "buy" ? "positive" : "negative"}>
      {side}
    </Badge>
  )
}

function EmptyRow({
  columns,
  message,
}: {
  columns: number
  message: string
}) {
  return (
    <TableRow>
      <TableCell
        colSpan={columns}
        className="h-28 text-center text-xs text-muted-foreground"
      >
        {message}
      </TableCell>
    </TableRow>
  )
}

/** Present persisted execution attempts, orders, aggregate fills and recoveries. */
export function OrdersPage() {
  const { snapshot } = useExecutionActivity()
  const { runtime } = useRuntime()
  const orders = snapshot?.orders ?? []
  const trades = snapshot?.trades ?? []
  const journals = snapshot?.journals ?? []
  const recoveries = snapshot?.recoveries ?? []
  const terminalJournals = journals.filter(
    (journal) =>
      ["completed", "recovered"].includes(journal.status) &&
      journal.netLockedPnlUsd !== null,
  )
  const terminalNet = terminalJournals.reduce(
    (total, journal) => total + (journal.netLockedPnlUsd ?? 0),
    0,
  )
  const regularMarkets = new Map(
    (runtime?.regular_markets ?? []).map((market) => [
      market.monitor_key,
      [...market.markets].sort((left, right) =>
        right.title.length - left.title.length,
      )[0]?.title ?? "Regular market",
    ]),
  )

  return (
    <>
      <PageHeader
        eyebrow="Execution"
        title="Orders & fills"
        description="Persisted order lifecycle, two-leg execution attempts, trading fees, inventory gas and exposure recoveries."
      />

      <ExecutionFeedStatus />

      <section className="mb-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4 2xl:grid-cols-7">
        {[
          {
            label: "Closed execution net PnL",
            value: terminalJournals.length ? signedUsd(terminalNet) : "—",
            icon: ReceiptText,
          },
          {
            label: "Trading fees",
            value: snapshot ? `$${formatPrice(snapshot.tradingFeesUsd)}` : "—",
            icon: ReceiptText,
          },
          {
            label: "Gas (inventory ops)",
            value: snapshot ? `$${formatPrice(snapshot.gasUsd)}` : "—",
            icon: Fuel,
          },
          { label: "Execution attempts", value: journals.length, icon: Activity },
          { label: "Orders", value: orders.length, icon: ListChecks },
          { label: "Aggregate fills", value: trades.length, icon: ReceiptText },
          { label: "Recoveries", value: recoveries.length, icon: RefreshCcw },
        ].map(({ label, value, icon: Icon }) => (
          <Card key={label}>
            <CardContent className="flex items-center justify-between p-4">
              <div>
                <p className="text-xs text-muted-foreground">{label}</p>
                <p className="numeric mt-1 text-2xl font-semibold">{value}</p>
              </div>
              <Icon className="size-5 text-primary" />
            </CardContent>
          </Card>
        ))}
      </section>

      <Card className="mb-4">
        <CardHeader className="flex-row items-start justify-between">
          <div>
            <CardTitle>Execution attempts</CardTitle>
            <CardDescription>
              Journal state for both venue legs and any remaining exposure.
            </CardDescription>
          </div>
          <Badge variant="outline">{journals.length}</Badge>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Updated</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Market</TableHead>
                <TableHead>First leg</TableHead>
                <TableHead>Hedge leg</TableHead>
                <TableHead className="text-right">Trading fees (USD)</TableHead>
                <TableHead className="text-right">End-to-end net / return</TableHead>
                <TableHead className="text-right">Residual</TableHead>
                <TableHead>Error</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {!journals.length ? (
                <EmptyRow
                  columns={9}
                  message="No execution attempts have been persisted."
                />
              ) : (
                journals.map((journal) => {
                  const capital = deployedCapital(journal)
                  const returnRate =
                    capital === null || journal.netLockedPnlUsd === null
                      ? null
                      : journal.netLockedPnlUsd / capital

                  const recovery = recoveries.find(
                    (value) => value.executionId === journal.executionId,
                  )

                  return (
                    <Fragment key={journal.executionId}>
                    <TableRow>
                    <TableCell className="text-xs text-muted-foreground">
                      {formatTimestamp(journal.updatedAt)}
                    </TableCell>
                    <TableCell>
                      <ExecutionStateBadge status={journal.status} />
                    </TableCell>
                    <TableCell className="w-56 max-w-56 whitespace-normal text-xs">
                      <div
                        className="line-clamp-3 break-words"
                        title={
                          regularMarkets.get(journal.monitorKey ?? "") ??
                          undefined
                        }
                      >
                        {journal.monitorType === "cycle" && journal.underlying &&
                        journal.intervalSeconds ? (
                          <>
                            <span className="font-semibold">
                              {journal.underlying}
                            </span>{" "}
                            <span className="text-muted-foreground">
                              {intervalLabel(journal.intervalSeconds)}
                            </span>
                          </>
                        ) : (
                          regularMarkets.get(journal.monitorKey ?? "") ?? "—"
                        )}
                      </div>
                    </TableCell>
                    {[journal.leg1, journal.leg2].map((leg) => (
                      <TableCell key={leg.clientOrderId}>
                        <div className="flex items-center gap-2">
                          <span className="text-xs font-semibold">
                            {leg.venueId}
                          </span>
                          <SideBadge side={leg.side} />
                        </div>
                        <p className="numeric mt-1 font-mono text-[11px] text-muted-foreground">
                          {formatQuantity(leg.filledQuantity)} /{" "}
                          {formatQuantity(leg.quantity)} @{" "}
                          {formatPrice(
                            leg.averageFillPrice ?? leg.limitPrice,
                          )}
                        </p>
                        <p className="numeric mt-1 font-mono text-[11px] text-muted-foreground">
                          Fee: {leg.feeAmount === null
                            ? "pending"
                            : `${formatPrice(leg.feeAmount)} ${leg.feeCurrency ?? ""}`}
                        </p>
                      </TableCell>
                    ))}
                    <TableCell className="numeric text-right font-mono text-xs">
                      {journal.totalFeeSettlementCostUsd === null
                        ? journal.status === "completed"
                          ? "pending"
                          : "—"
                        : `$${formatPrice(journal.totalFeeSettlementCostUsd)}`}
                    </TableCell>
                    <TableCell
                      className={`numeric text-right font-mono text-xs ${
                        journal.netLockedPnlUsd === null
                          ? ""
                          : journal.netLockedPnlUsd >= 0
                            ? "text-emerald-300"
                            : "text-rose-300"
                      }`}
                      title={
                        journal.grossLockedPnlUsd === null
                          ? undefined
                          : `Gross locked PnL: $${formatPrice(journal.grossLockedPnlUsd)}`
                      }
                    >
                      {journal.netLockedPnlUsd === null
                        ? journal.status === "completed"
                          ? "pending"
                          : "—"
                        : (
                          <>
                            <div>${formatPrice(journal.netLockedPnlUsd)}</div>
                            {capital !== null && returnRate !== null ? (
                              <div className="mt-1 text-[10px] text-muted-foreground">
                                {formatPercent(returnRate)} net on ${formatPrice(capital)}
                              </div>
                            ) : null}
                          </>
                        )}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {formatQuantity(journal.residualQuantity)}
                    </TableCell>
                    <TableCell
                      className="max-w-64 truncate text-xs text-rose-300"
                      title={journal.lastError ?? undefined}
                    >
                      {journal.lastError ?? "—"}
                    </TableCell>
                    </TableRow>
                    {journal.status === "recovered" ? (
                      <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={9} className="whitespace-normal py-2">
                          <RecoveryDetails
                            journal={journal}
                            recovery={recovery}
                            orders={orders}
                          />
                        </TableCell>
                      </TableRow>
                    ) : null}
                    {journal.manualResolution ? (
                      <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={9} className="whitespace-normal py-2">
                          <ManualResolutionDetails
                            journal={journal}
                            resolution={journal.manualResolution}
                          />
                        </TableCell>
                      </TableRow>
                    ) : null}
                    {journal.status === "needs_review" ? (
                      <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={9} className="whitespace-normal py-2">
                          <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-amber-400/20 bg-amber-400/[0.04] p-4">
                            <div>
                              <p className="text-sm font-semibold">Human resolution required</p>
                              <p className="mt-1 text-xs text-muted-foreground">
                                After closing or settling the residual externally, record the actual result here.
                              </p>
                              {runtime?.trading_enabled ? (
                                <p className="mt-1 text-xs text-amber-300">
                                  Disable live trading before recording the resolution.
                                </p>
                              ) : null}
                            </div>
                            <ManualResolutionDialog
                              journal={journal}
                              recovery={recovery}
                              tradingEnabled={runtime?.trading_enabled === true}
                            />
                          </div>
                        </TableCell>
                      </TableRow>
                    ) : null}
                    </Fragment>
                  )
                })
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <section className="grid gap-4 2xl:grid-cols-2">
        <Card>
          <CardHeader className="flex-row items-start justify-between">
            <div>
              <CardTitle>Orders</CardTitle>
              <CardDescription>
                Terminal and non-terminal snapshots persisted after execution.
              </CardDescription>
            </div>
            <Badge variant="outline">{orders.length}</Badge>
          </CardHeader>
          <CardContent className="px-2 sm:px-5">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Updated</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Side</TableHead>
                  <TableHead>Contract</TableHead>
                  <TableHead className="text-right">Limit</TableHead>
                  <TableHead className="text-right">Filled</TableHead>
                  <TableHead className="text-right">Average</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {!orders.length ? (
                  <EmptyRow
                    columns={7}
                    message="No order snapshots have been persisted."
                  />
                ) : (
                  orders.map((order, index) => (
                    <TableRow
                      key={
                        order.orderId ??
                        order.clientOrderId ??
                        `${order.contractId}:${index}`
                      }
                    >
                      <TableCell className="text-xs text-muted-foreground">
                        {formatTimestamp(order.updatedAt)}
                      </TableCell>
                      <TableCell>
                        <ExecutionStateBadge status={order.status} />
                      </TableCell>
                      <TableCell>
                        <SideBadge side={order.side} />
                      </TableCell>
                      <TableCell
                        className="max-w-48 truncate font-mono text-[11px]"
                        title={order.contractId}
                      >
                        {order.contractId}
                      </TableCell>
                      <TableCell className="numeric text-right font-mono text-xs">
                        {order.limitPrice === null
                          ? "—"
                          : formatPrice(order.limitPrice)}
                      </TableCell>
                      <TableCell className="numeric text-right font-mono text-xs">
                        {formatQuantity(order.filledQuantity)} /{" "}
                        {formatQuantity(order.quantity)}
                      </TableCell>
                      <TableCell className="numeric text-right font-mono text-xs">
                        {order.averagePrice === null
                          ? "—"
                          : formatPrice(order.averagePrice)}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex-row items-start justify-between">
            <div>
              <CardTitle>Aggregate fills</CardTitle>
              <CardDescription>
                One persisted trade per order until venue fill IDs are exposed.
              </CardDescription>
            </div>
            <Badge variant="outline">{trades.length}</Badge>
          </CardHeader>
          <CardContent className="px-2 sm:px-5">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Executed</TableHead>
                  <TableHead>Side</TableHead>
                  <TableHead>Contract</TableHead>
                  <TableHead className="text-right">Quantity</TableHead>
                  <TableHead className="text-right">Price</TableHead>
                  <TableHead className="text-right">Trading fee</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {!trades.length ? (
                  <EmptyRow
                    columns={6}
                    message="No filled order has produced a persisted trade."
                  />
                ) : (
                  trades.map((trade) => (
                    <TableRow key={trade.tradeId}>
                      <TableCell className="text-xs text-muted-foreground">
                        {formatTimestamp(trade.executedAt)}
                      </TableCell>
                      <TableCell>
                        <SideBadge side={trade.side} />
                      </TableCell>
                      <TableCell
                        className="max-w-48 truncate font-mono text-[11px]"
                        title={trade.contractId}
                      >
                        {trade.contractId}
                      </TableCell>
                      <TableCell className="numeric text-right font-mono text-xs">
                        {formatQuantity(trade.quantity)}
                      </TableCell>
                      <TableCell className="numeric text-right font-mono text-xs">
                        {formatPrice(trade.price)}
                      </TableCell>
                      <TableCell className="numeric text-right font-mono text-xs">
                        {trade.feeAmount === null
                          ? "—"
                          : `${formatPrice(trade.feeAmount)} ${
                              trade.feeCurrency ?? ""
                            }`}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      </section>

      <Card className="mt-4">
        <CardHeader className="flex-row items-start justify-between">
          <div>
            <CardTitle>Exposure recoveries</CardTitle>
            <CardDescription>
              Persisted attempts to neutralize an unmatched venue fill.
            </CardDescription>
          </div>
          <Badge variant="outline">{recoveries.length}</Badge>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Updated</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Route</TableHead>
                <TableHead>Venue</TableHead>
                <TableHead>Side</TableHead>
                <TableHead>Contract</TableHead>
                <TableHead className="text-right">Filled / target</TableHead>
                <TableHead className="text-right">Average / limit</TableHead>
                <TableHead className="text-right">Fees</TableHead>
                <TableHead className="text-right">Estimated net</TableHead>
                <TableHead className="text-right">Actual net</TableHead>
                <TableHead>Error</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {!recoveries.length ? (
                <EmptyRow
                  columns={12}
                  message="No exposure recovery has been required."
                />
              ) : (
                recoveries.map((recovery) => (
                  <TableRow key={recovery.recoveryId}>
                    <TableCell className="text-xs text-muted-foreground">
                      {formatTimestamp(recovery.updatedAt)}
                    </TableCell>
                    <TableCell>
                      <ExecutionStateBadge status={recovery.status} />
                    </TableCell>
                    <TableCell className="text-xs capitalize">
                      {recovery.route?.replaceAll("_", " ") ?? "—"}
                    </TableCell>
                    <TableCell className="text-xs font-semibold">
                      {recovery.venueId}
                    </TableCell>
                    <TableCell>
                      <SideBadge side={recovery.side} />
                    </TableCell>
                    <TableCell
                      className="max-w-56 truncate font-mono text-[11px]"
                      title={recovery.contractId}
                    >
                      {recovery.contractId}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {formatQuantity(recovery.filledQuantity)} /{" "}
                      {formatQuantity(recovery.quantity)}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {recovery.averagePrice === null
                        ? "—"
                        : formatPrice(recovery.averagePrice)}{" "}
                      / {formatPrice(recovery.limitPrice)}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {recoveryFees(recovery) === null
                        ? "—"
                        : formatPrice(recoveryFees(recovery)!)}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {recovery.estimatedNetResult === null
                        ? "—"
                        : formatPrice(recovery.estimatedNetResult)}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {recovery.actualNetResult === null
                        ? "—"
                        : formatPrice(recovery.actualNetResult)}
                    </TableCell>
                    <TableCell
                      className="max-w-64 truncate text-xs text-rose-300"
                      title={recovery.lastError ?? undefined}
                    >
                      {recovery.lastError ?? "—"}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </>
  )
}
