import { CircleDollarSign, Layers3 } from "lucide-react"

import {
  ExecutionFeedStatus,
  ExecutionStateBadge,
} from "@/components/execution/execution-status"
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
import { useExecutionActivity } from "@/features/execution/execution-activity-provider"
import {
  formatPrice,
  formatQuantity,
  formatTimestamp,
} from "@/lib/formatters"

/** Present positions derived from the persisted aggregate trade history. */
export function PositionsPage() {
  const { snapshot } = useExecutionActivity()
  const positions = snapshot?.positions ?? []
  const openPositions = positions.filter(
    (position) => position.side.toLowerCase() !== "flat",
  )

  return (
    <>
      <PageHeader
        eyebrow="Portfolio"
        title="Positions"
        description="Current contract exposure derived by FastAPI from persisted aggregate trades."
      />

      <ExecutionFeedStatus />

      <section className="mb-4 grid gap-3 sm:grid-cols-2">
        <Card>
          <CardContent className="flex items-center justify-between p-4">
            <div>
              <p className="text-xs text-muted-foreground">Open positions</p>
              <p className="numeric mt-1 text-2xl font-semibold">
                {openPositions.length}
              </p>
            </div>
            <Layers3 className="size-5 text-primary" />
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center justify-between p-4">
            <div>
              <p className="text-xs text-muted-foreground">Tracked contracts</p>
              <p className="numeric mt-1 text-2xl font-semibold">
                {positions.length}
              </p>
            </div>
            <CircleDollarSign className="size-5 text-primary" />
          </CardContent>
        </Card>
      </section>

      <Card>
        <CardHeader className="flex-row items-start justify-between">
          <div>
            <CardTitle>Contract positions</CardTitle>
            <CardDescription>
              Average entry and net quantity after all persisted buys and sells.
            </CardDescription>
          </div>
          <Badge variant="outline">{positions.length}</Badge>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Updated</TableHead>
                <TableHead>Side</TableHead>
                <TableHead>Contract</TableHead>
                <TableHead className="text-right">Quantity</TableHead>
                <TableHead className="text-right">Average entry</TableHead>
                <TableHead>Portfolio</TableHead>
                <TableHead>Opened</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {!positions.length ? (
                <TableRow>
                  <TableCell
                    colSpan={7}
                    className="h-36 text-center text-xs text-muted-foreground"
                  >
                    No position has been derived from persisted trades.
                  </TableCell>
                </TableRow>
              ) : (
                positions.map((position) => (
                  <TableRow key={position.positionId}>
                    <TableCell className="text-xs text-muted-foreground">
                      {formatTimestamp(position.updatedAt)}
                    </TableCell>
                    <TableCell>
                      <ExecutionStateBadge status={position.side} />
                    </TableCell>
                    <TableCell
                      className="max-w-72 truncate font-mono text-[11px]"
                      title={position.contractId}
                    >
                      {position.contractId}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {formatQuantity(position.quantity)}
                    </TableCell>
                    <TableCell className="numeric text-right font-mono text-xs">
                      {position.averageEntryPrice === null
                        ? "—"
                        : formatPrice(position.averageEntryPrice)}
                    </TableCell>
                    <TableCell className="text-xs">
                      {position.portfolioId ?? "default"}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground">
                      {formatTimestamp(position.openedAt)}
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
