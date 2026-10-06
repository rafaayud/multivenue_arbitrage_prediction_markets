import { RadioTower } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  formatPercent,
  formatPrice,
  formatQuantity,
  formatTimestamp,
  intervalLabel,
} from "@/lib/formatters"
import type { ArbitrageOpportunity } from "@/types/arbitrage"

/** Render filtered arbitrage opportunities as paired venue legs. */
export function OpportunityTable({
  opportunities,
  limit,
}: {
  opportunities: ArbitrageOpportunity[]
  limit?: number
}) {
  const visible = limit ? opportunities.slice(0, limit) : opportunities

  if (!visible.length) {
    return (
      <div className="flex min-h-56 flex-col items-center justify-center px-6 text-center">
        <div className="rounded-full border border-border bg-secondary p-3 text-muted-foreground">
          <RadioTower className="size-5" />
        </div>
        <p className="mt-4 text-sm font-medium">Waiting for opportunities</p>
        <p className="mt-1 max-w-sm text-xs leading-relaxed text-muted-foreground">
          Connect an event from the catalog. Matching two-leg opportunities
          will appear here as the selected markets update.
        </p>
      </div>
    )
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Time</TableHead>
          <TableHead>Market</TableHead>
          <TableHead>Type</TableHead>
          <TableHead>Venue / action</TableHead>
          <TableHead className="text-right">Price</TableHead>
          <TableHead className="text-right">Quantity</TableHead>
          <TableHead className="text-right">Edge</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {visible.map((opportunity) => (
          <TableRow key={opportunity.id}>
            <TableCell className="text-xs text-muted-foreground">
              {formatTimestamp(opportunity.generatedAt)}
            </TableCell>
            <TableCell>
              <span className="font-semibold">{opportunity.marketLabel}</span>
              {opportunity.intervalSeconds ? (
                <span className="ml-1.5 text-xs text-muted-foreground">
                  {intervalLabel(opportunity.intervalSeconds)}
                </span>
              ) : (
                <span className="ml-1.5 text-xs text-muted-foreground">
                  Regular
                </span>
              )}
            </TableCell>
            <TableCell>
              <Badge
                variant={
                  opportunity.side === "LONG"
                    ? "positive"
                    : opportunity.side === "SHORT"
                      ? "negative"
                      : "warning"
                }
              >
                {opportunity.side}
              </Badge>
            </TableCell>
            <TableCell>
              <div className="space-y-1.5">
                {opportunity.signals.map((signal) => (
                  <div
                    key={signal.contractId}
                    className="flex items-center gap-2"
                  >
                    <span className="w-20 truncate text-xs font-medium capitalize">
                      {signal.venueId}
                    </span>
                    <Badge
                      variant={
                        signal.direction === "buy" ? "positive" : "negative"
                      }
                    >
                      {signal.direction}
                    </Badge>
                  </div>
                ))}
              </div>
            </TableCell>
            <TableCell className="numeric text-right font-mono text-xs">
              <div className="space-y-2">
                {opportunity.signals.map((signal) => (
                  <div key={signal.contractId}>
                    {formatPrice(signal.limitPrice)}
                  </div>
                ))}
              </div>
            </TableCell>
            <TableCell className="numeric text-right font-mono text-xs">
              <div className="space-y-2">
                {opportunity.signals.map((signal) => (
                  <div key={signal.contractId}>
                    {formatQuantity(signal.quantity)}
                  </div>
                ))}
              </div>
            </TableCell>
            <TableCell className="numeric text-right font-mono text-xs text-primary">
              <div className="space-y-2">
                {opportunity.signals.map((signal) => (
                  <div key={signal.contractId}>
                    {formatPercent(signal.edge)}
                  </div>
                ))}
              </div>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}
