import { useMemo, useState } from "react"

import { PageHeader } from "@/components/layout/page-header"
import { OpportunityTable } from "@/components/signals/opportunity-table"
import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { useArbitrageStream } from "@/features/arbitrage/arbitrage-stream-provider"
import {
  filterOpportunities,
  type ArbitrageFilters,
} from "@/features/arbitrage/filters"

/** Present selected-event arbitrage signals with long and short filters. */
export function SignalsPage() {
  const stream = useArbitrageStream()

  const [filters, setFilters] = useState<ArbitrageFilters>({

    side: "ALL",
  })
  const opportunities = useMemo(
    () => filterOpportunities(stream.opportunities, filters),
    [stream.opportunities, filters],
  )

  return (
    <>
      <PageHeader
        eyebrow="Signal tape"
        title="Arbitrage opportunities"
        description="A bounded feed of the latest two-leg opportunities received from FastAPI."
      />

      <Card>
        <CardHeader className="gap-4 xl:flex-row xl:items-center xl:justify-between">
          <div>
            <div className="flex items-center gap-2">
              <CardTitle>Live signal feed</CardTitle>
              <Badge
                variant={
                  stream.connectionStatus === "connected"
                    ? "positive"
                    : "warning"
                }
              >
                {stream.connectionStatus}
              </Badge>
            </div>
            <CardDescription>
              The latest 200 saved detections and live signals. Past signals may no longer be tradable.
            </CardDescription>
          </div>

          <div className="flex flex-wrap gap-2">

            <Select
              value={filters.side}
              onValueChange={(value) =>
                setFilters((current) => ({
                  ...current,
                  side: value as ArbitrageFilters["side"],
                }))
              }
            >
              <SelectTrigger aria-label="Filter by opportunity type">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">Long & short</SelectItem>
                <SelectItem value="LONG">Long</SelectItem>
                <SelectItem value="SHORT">Short</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          {stream.historyError && <p role="alert" className="mb-3 text-xs text-amber-600 dark:text-amber-300">{stream.historyError}</p>}
          <OpportunityTable opportunities={opportunities} />
        </CardContent>
      </Card>
    </>
  )
}
