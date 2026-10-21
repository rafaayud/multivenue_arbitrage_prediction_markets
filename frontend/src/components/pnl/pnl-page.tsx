import { useState } from "react"
import { Clock3 } from "lucide-react"

import { ExecutionFeedStatus } from "@/components/execution/execution-status"
import { PageHeader } from "@/components/layout/page-header"
import { PnlChart } from "@/components/pnl/pnl-chart"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { VenueLogo } from "@/components/venues/venue-logo"
import { usePnl } from "@/features/pnl/use-pnl"
import { formatPrice, formatTimestamp } from "@/lib/formatters"

function formatUsd(value: string | null | undefined): string {
  if (value == null || !Number.isFinite(Number(value))) return "N/A"
  const parsed = Number(value)
  return `${parsed < 0 ? "-" : ""}$${formatPrice(Math.abs(parsed))}`
}

function tone(value: string | null | undefined): string {
  if (value == null || Number(value) === 0) return "text-muted-foreground"
  return Number(value) > 0 ? "text-emerald-700 dark:text-emerald-300" : "text-rose-700 dark:text-rose-400"
}

/** Count completed arbitrage results immediately and show token holdings separately. */
export function PnlPage() {
  const { pnl, loading, error, updatedAt } = usePnl("bot_ledger", "ALL")
  const [page, setPage] = useState(0)
  const result = pnl?.terminal_executions
  const executions = result?.executions ?? []
  const lastPage = Math.max(0, Math.ceil(executions.length / 10) - 1)
  const currentPage = Math.min(page, lastPage)
  const missingExecutions = result?.unpriced_terminal_executions ?? 0
  const missingGas = result?.unpriced_inventory_operations ?? 0
  const incomplete = missingExecutions > 0 || missingGas > 0
  const positions = pnl?.positions.filter((position) => Number(position.quantity) !== 0) ?? []
  const breakdown = [
    { name: "Gross execution result", note: "Locked arbitrages and recovered executions", value: result?.gross_pnl_usd, operator: "+" },
    { name: "Trading fees", note: "Actual fees on the executions counted above", value: result?.fees_usd, operator: "−" },
    { name: "Gas spent", note: "Recorded inventory transaction costs, all time", value: result?.gas_usd, operator: "−" },
  ]

  return (
    <>
      <PageHeader eyebrow="Portfolio" title="Arbitrage PnL" description="Your result when an arbitrage completes. No waiting for the event to settle." />
      <ExecutionFeedStatus />
      {error && <div role="alert" className="mb-4 rounded-lg border border-destructive/25 bg-destructive/10 px-4 py-3 text-sm text-destructive">{error}</div>}

      <Card className="mb-4 overflow-hidden border-primary/15 bg-gradient-to-br from-primary/[0.07] via-card to-card">
        <CardContent className="p-5 sm:p-7">
          <div className="grid gap-7 lg:grid-cols-2 lg:items-center">
            <div>
              <div className="mb-4 flex flex-wrap gap-2">
                <Badge variant="outline">All time · USD</Badge>
                {result && <Badge variant={incomplete ? "warning" : "positive"}>{incomplete ? "Incomplete costs or fills" : "Recorded costs included"}</Badge>}
                {loading && <Badge variant="outline">Updating…</Badge>}
              </div>
              <p className="text-xs font-medium uppercase tracking-[0.16em] text-muted-foreground">Net arbitrage PnL</p>
              <p className={`numeric mt-2 text-4xl font-semibold sm:text-5xl ${tone(result?.net_after_gas_usd)}`}>
                {loading && !pnl ? "…" : formatUsd(result?.net_after_gas_usd)}
              </p>
              <p className="mt-3 text-sm font-medium">Trading fees and recorded gas are already deducted.</p>
              <p className="mt-2 max-w-lg text-sm leading-relaxed text-muted-foreground">
                Counted as soon as both legs complete, even while you still hold the tokens. Includes gains and losses from recovered executions.
              </p>
              <p className="mt-3 text-xs text-muted-foreground">{result?.priced_terminal_executions ?? 0} priced executions · Result locked, not necessarily collected in cash.</p>
            </div>
            <div className="rounded-xl border border-border/80 bg-background/55 p-4 sm:p-5">
              <h2 className="mb-4 text-sm font-semibold">How your result adds up</h2>
              <dl className="space-y-4">
                {breakdown.map(({ name, note, value, operator }) => (
                  <div key={name} className="flex items-start gap-3">
                    <span className="w-4 shrink-0 text-muted-foreground" aria-hidden="true">{operator}</span>
                    <dt className="min-w-0 flex-1 text-sm">{name}<span className="mt-1 block text-[11px] text-muted-foreground">{note}</span></dt>
                    <dd className="numeric shrink-0 text-sm font-semibold">{formatUsd(value)}</dd>
                  </div>
                ))}
                <div className="flex items-center gap-3 border-t border-border pt-4">
                  <span className="w-4 shrink-0 text-muted-foreground" aria-hidden="true">=</span>
                  <dt className="flex-1 text-sm font-semibold">Net after costs</dt>
                  <dd className={`numeric text-lg font-semibold ${tone(result?.net_after_gas_usd)}`}>{formatUsd(result?.net_after_gas_usd)}</dd>
                </div>
              </dl>
            </div>
          </div>
          {incomplete && (
            <div role="status" className="mt-5 space-y-1 rounded-lg bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-300">
              {missingExecutions > 0 && <p>{missingExecutions} executions are excluded because fills or fees are incomplete.</p>}
              {missingGas > 0 && <p>{missingGas} inventory operations have unknown USD costs. Their gas is not deducted yet; this result is provisional.</p>}
            </div>
          )}
          <div className="mt-5 flex flex-wrap justify-between gap-3 border-t border-border/70 pt-4 text-[11px] text-muted-foreground">
            <p>Gas includes inventory preparation for future trades. It is deducted once, across the portfolio.</p>
            <p className="flex items-center gap-1.5"><Clock3 className="size-3.5" />Updated {updatedAt ? formatTimestamp(updatedAt) : pnl ? formatTimestamp(pnl.generated_at) : "—"}</p>
          </div>
        </CardContent>
      </Card>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Cumulative net PnL</CardTitle>
          <CardDescription>Each step is a completed execution or recorded gas charge. Green is profit; red is loss. Fees and gas are included.</CardDescription>
        </CardHeader>
        <CardContent><PnlChart series={result?.net_after_gas_series ?? []} /></CardContent>
      </Card>

      <Card className="mb-4">
        <CardHeader>
          <CardTitle>Execution results</CardTitle>
          <CardDescription>Profit is counted at completion, before event settlement. Each row includes its trading fees; portfolio gas is deducted in the total above.</CardDescription>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <Table>
            <TableHeader><TableRow><TableHead>Completed</TableHead><TableHead>Market / route</TableHead><TableHead>Status</TableHead><TableHead className="text-right">Gross</TableHead><TableHead className="text-right">Fees</TableHead><TableHead className="text-right">After trading fees</TableHead></TableRow></TableHeader>
            <TableBody>
              {!executions.length ? <TableRow><TableCell colSpan={6} className="h-24 text-center text-muted-foreground">{loading ? "Loading executions…" : "No completed executions yet."}</TableCell></TableRow> : executions.slice(currentPage * 10, currentPage * 10 + 10).map((execution) => (
                <TableRow key={execution.execution_id}>
                  <TableCell className="whitespace-nowrap text-xs text-muted-foreground">{formatTimestamp(execution.updated_at)}</TableCell>
                  <TableCell>
                    <p className="font-medium" title={execution.execution_id}>{execution.underlying ?? execution.monitor_key ?? "Arbitrage"}</p>
                    <div className="mt-1 flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground"><VenueLogo venue={execution.leg1.venue_id} />{execution.leg1.venue_id}<span>↔</span><VenueLogo venue={execution.leg2.venue_id} />{execution.leg2.venue_id}</div>
                  </TableCell>
                  <TableCell><Badge variant={execution.net_locked_pnl_usd == null ? "warning" : execution.status === "completed" ? "positive" : "outline"}>{execution.net_locked_pnl_usd == null ? "Incomplete data" : execution.status === "completed" ? "Locked" : "Recovered"}</Badge></TableCell>
                  <TableCell className="numeric text-right">{formatUsd(execution.gross_locked_pnl_usd)}</TableCell>
                  <TableCell className="numeric text-right">{formatUsd(execution.total_fee_settlement_cost_usd)}</TableCell>
                  <TableCell className={`numeric text-right font-semibold ${tone(execution.net_locked_pnl_usd)}`}>{formatUsd(execution.net_locked_pnl_usd)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          {lastPage > 0 && <div className="mt-4 flex items-center justify-end gap-3 text-xs"><span>Page {currentPage + 1} of {lastPage + 1}</span><Button size="sm" variant="outline" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>Previous</Button><Button size="sm" variant="outline" disabled={currentPage === lastPage} onClick={() => setPage(currentPage + 1)}>Next</Button></div>}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-start justify-between gap-3">
          <div><CardTitle>Tokens held</CardTitle><CardDescription>Current inventory by venue and token. These balances are separate from the execution results above.</CardDescription></div>
          <Badge variant="outline">{positions.length} positions</Badge>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <Table>
            <TableHeader><TableRow><TableHead>Venue / token</TableHead><TableHead>Side</TableHead><TableHead className="text-right">Quantity</TableHead><TableHead className="text-right">Average entry</TableHead><TableHead className="text-right">Current price</TableHead></TableRow></TableHeader>
            <TableBody>
              {!positions.length ? <TableRow><TableCell colSpan={5} className="h-24 text-center text-muted-foreground">{loading ? "Loading inventory…" : "No token positions recorded."}</TableCell></TableRow> : positions.map((position) => (
                <TableRow key={position.position_id}>
                  <TableCell className="max-w-72"><div className="flex items-center gap-2 font-medium"><VenueLogo venue={position.venue_id} />{position.venue_id}</div><p className="truncate text-[11px] text-muted-foreground" title={position.contract_id}>{position.contract_id}</p></TableCell>
                  <TableCell><Badge variant="outline">{position.side}</Badge></TableCell>
                  <TableCell className="numeric text-right">{formatPrice(Number(position.quantity))}</TableCell>
                  <TableCell className="numeric text-right">{formatUsd(position.average_entry_price)}</TableCell>
                  <TableCell className="numeric text-right">{formatUsd(position.current_price)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </CardContent>
      </Card>
    </>
  )
}
