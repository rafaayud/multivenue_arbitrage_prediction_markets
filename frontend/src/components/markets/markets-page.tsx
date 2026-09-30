import { Fragment, useMemo, useState } from "react"

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
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { useMarketMatches } from "@/features/markets/use-market-matches"
import { useRuntime } from "@/features/runtime/use-runtime"
import { intervalLabel } from "@/lib/formatters"
import type { MarketMatchPair } from "@/types/api"

interface MarketRow {
  key: string
  label: string
  family: "crypto" | "finance" | "regular"
  intervalSeconds: number | null
  pairs: MarketMatchPair[]
}

/** Present monitored markets as a searchable, family-grouped catalog. */
export function MarketsPage() {
  const { matches, loading, error, updatedAt } = useMarketMatches()
  const runtime = useRuntime()
  const regularMarkets = runtime.runtime?.regular_markets
  const [query, setQuery] = useState("")
  const [family, setFamily] = useState("ALL")
  const [interval, setInterval] = useState("ALL")
  const [status, setStatus] = useState("ALL")

  const rows = useMemo<MarketRow[]>(
    () => [
      ...matches.map((snapshot) => ({
        key: snapshot.monitor_key,
        label: snapshot.underlying,
        family: snapshot.family,
        intervalSeconds: snapshot.interval_seconds,
        pairs: snapshot.pairs,
      })),
      ...(regularMarkets ?? []).map((candidate) => ({
        key: candidate.monitor_key,
        label: candidate.markets[0]?.title ?? "Regular market",
        family: "regular" as const,
        intervalSeconds: null,
        pairs: candidate.pairs,
      })),
    ],
    [matches, regularMarkets],
  )
  const intervals = [...new Set(matches.map((item) => item.interval_seconds))].sort(
    (left, right) => left - right,
  )
  const normalizedQuery = query.trim().toLowerCase()
  const visible = rows.filter((row) => {
    const venues = row.pairs.flatMap((pair) => [
      pair.left.venue_id,
      pair.right.venue_id,
    ])
    return (
      (family === "ALL" || row.family === family) &&
      (interval === "ALL" || row.intervalSeconds === Number(interval)) &&
      (status === "ALL" ||
        (status === "MATCHED" ? row.pairs.length > 0 : row.pairs.length === 0)) &&
      (!normalizedQuery ||
        [row.label, row.family, ...venues]
          .join(" ")
          .toLowerCase()
          .includes(normalizedQuery))
    )
  })
  const grouped = (["crypto", "finance", "regular"] as const)
    .map(
      (rowFamily) =>
        [
          rowFamily,
          visible.filter((row) => row.family === rowFamily),
        ] as const,
    )
    .filter(([, familyRows]) => familyRows.length > 0)

  return (
    <>
      <PageHeader
        eyebrow="Discovery"
        title="Monitored markets"
        description="Recurring Up/Down cycles and explicitly monitored regular markets."
      />

      <Card>
        <CardHeader className="gap-4 xl:flex-row xl:items-center xl:justify-between">
          <div>
            <CardTitle>Market catalog</CardTitle>
            <CardDescription>
              One row per market; contract routes remain available on demand.
            </CardDescription>
          </div>
          <Badge variant="outline">
            {visible.length}/{rows.length} markets
          </Badge>
        </CardHeader>
        <CardContent className="px-2 sm:px-5">
          <div className="mb-4 flex flex-wrap gap-2 px-3 sm:px-0">
            <input
              aria-label="Search monitored markets"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search asset or venue"
              className="h-9 min-w-56 flex-1 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
            />
            <Select value={family} onValueChange={setFamily}>
              <SelectTrigger aria-label="Filter by market family">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">All families</SelectItem>
                <SelectItem value="crypto">Crypto</SelectItem>
                <SelectItem value="finance">Finance</SelectItem>
                <SelectItem value="regular">Regular</SelectItem>
              </SelectContent>
            </Select>
            <Select value={interval} onValueChange={setInterval}>
              <SelectTrigger aria-label="Filter by market interval">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">All cycles</SelectItem>
                {intervals.map((value) => (
                  <SelectItem key={value} value={String(value)}>
                    {intervalLabel(value)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={status} onValueChange={setStatus}>
              <SelectTrigger aria-label="Filter by match status">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">All statuses</SelectItem>
                <SelectItem value="MATCHED">Matched</SelectItem>
                <SelectItem value="EMPTY">No pairs</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {loading && !rows.length ? (
            <div className="space-y-3 px-3 pb-3">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : error && !rows.length ? (
            <p className="px-3 pb-3 text-xs text-rose-300">{error}</p>
          ) : !visible.length ? (
            <div className="min-h-56 px-4 py-14 text-center">
              <p className="text-sm font-medium">No markets match the filters</p>
              <p className="mx-auto mt-2 max-w-md text-xs leading-relaxed text-muted-foreground">
                Clear one or more filters or wait for the backend catalog.
              </p>
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Market</TableHead>
                  <TableHead>Cycle</TableHead>
                  <TableHead>Venues</TableHead>
                  <TableHead>Compatible routes</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {[...grouped].map(([rowFamily, familyRows]) => (
                  <Fragment key={rowFamily}>
                    <TableRow className="bg-secondary/35 hover:bg-secondary/35">
                      <TableCell
                        colSpan={5}
                        className="py-2 text-[10px] font-semibold uppercase tracking-[0.16em] text-muted-foreground"
                      >
                        {rowFamily} · {familyRows.length}
                      </TableCell>
                    </TableRow>
                    {familyRows.map((row) => {
                      const venues = [
                        ...new Set(
                          row.pairs.flatMap((pair) => [
                            pair.left.venue_id,
                            pair.right.venue_id,
                          ]),
                        ),
                      ]
                      return (
                        <TableRow key={row.key}>
                          <TableCell className="max-w-72">
                            <span className="block truncate font-semibold" title={row.label}>
                              {row.label}
                            </span>
                          </TableCell>
                          <TableCell>
                            {row.intervalSeconds ? (
                              <Badge variant="outline">
                                {intervalLabel(row.intervalSeconds)}
                              </Badge>
                            ) : (
                              <span className="text-xs text-muted-foreground">Manual</span>
                            )}
                          </TableCell>
                          <TableCell className="capitalize">
                            {venues.length ? venues.join(" · ") : "—"}
                          </TableCell>
                          <TableCell>
                            {row.pairs.length ? (
                              <details>
                                <summary className="cursor-pointer text-xs font-medium text-primary">
                                  {row.pairs.length} route{row.pairs.length === 1 ? "" : "s"}
                                </summary>
                                <div className="mt-2 space-y-1.5">
                                  {row.pairs.map((pair, index) => (
                                    <p
                                      key={`${pair.left.id}:${pair.right.id}`}
                                      className="text-[11px] capitalize text-muted-foreground"
                                      title={`${pair.left.id} ↔ ${pair.right.id}`}
                                    >
                                      Route {index + 1}: {pair.left.venue_id} ↔ {pair.right.venue_id}
                                    </p>
                                  ))}
                                </div>
                              </details>
                            ) : (
                              <span className="text-xs text-muted-foreground">No routes</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <Badge variant={row.pairs.length ? "positive" : "outline"}>
                              {row.pairs.length ? "Matched" : "Waiting"}
                            </Badge>
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </Fragment>
                ))}
              </TableBody>
            </Table>
          )}
          {error && rows.length > 0 && (
            <p className="px-3 pt-3 text-[10px] text-rose-300">{error}</p>
          )}
          {updatedAt && (
            <p className="px-3 pt-3 text-[10px] text-muted-foreground">
              Last refreshed {updatedAt.toLocaleTimeString()}
            </p>
          )}
        </CardContent>
      </Card>
    </>
  )
}
