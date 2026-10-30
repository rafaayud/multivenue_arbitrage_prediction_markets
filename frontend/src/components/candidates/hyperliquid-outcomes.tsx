import { type FormEvent, useEffect, useState } from "react"
import { PageHeader } from "@/components/layout/page-header"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { apiClient } from "@/lib/api-client"
import { formatTimestamp } from "@/lib/formatters"
import type { HyperliquidBook, HyperliquidBooks, HyperliquidCatalog, HyperliquidMarket } from "@/types/hyperliquid"

/** Browse native YES/NO questions and inspect public depth without trading controls. */
export function HyperliquidOutcomes() {
  const [draft, setDraft] = useState("")
  const [search, setSearch] = useState("")
  const [catalog, setCatalog] = useState<HyperliquidCatalog | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<HyperliquidMarket | null>(null)
  const [reload, setReload] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    setError(null)
    setCatalog(null)
    setSelected(null)
    apiClient.hyperliquidMarkets(search, controller.signal).then((result) => {
      if (!controller.signal.aborted) setCatalog(result)
    }).catch((reason: unknown) => {
      if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "Outcome catalog unavailable")
    })
    return () => controller.abort()
  }, [search, reload])

  function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setSearch(draft.trim())
    setReload((value) => value + 1)
  }

  return (
    <>
      <PageHeader eyebrow="Hyperliquid · Public market data" title="Outcome explorer" description="Explore native YES/NO contracts and inspect both order books." />
      <div className="mb-5 flex flex-wrap items-center gap-3 rounded-xl border border-border bg-muted/30 p-4 text-sm">
        <Badge variant="outline">Trading disabled</Badge>
        {catalog?.network && <Badge variant="secondary">{catalog.network}</Badge>}
        <span className="text-muted-foreground">Market data only. These markets cannot submit orders or create inventory.</span>
      </div>
      <form onSubmit={submitSearch} className="mb-5 flex flex-wrap gap-2">
        <label className="sr-only" htmlFor="hyperliquid-search">Search Hyperliquid outcomes</label>
        <input id="hyperliquid-search" value={draft} onChange={(event) => setDraft(event.target.value)} placeholder="Search a question, e.g. FOMC or Bitcoin" type="search" maxLength={200} className="h-10 min-w-48 flex-1 rounded-lg border border-border bg-background px-3 text-sm" />
        <Button type="submit">Search outcomes</Button>
        <Button type="button" variant="outline" onClick={() => setReload((value) => value + 1)}>Refresh catalog</Button>
      </form>
      {error && <p role="alert" className="mb-4 text-sm text-destructive">{error}</p>}
      {!catalog && !error && <p role="status" className="text-sm text-muted-foreground">Loading outcomes…</p>}
      {catalog && !catalog.enabled && (
        <Card><CardContent className="pt-6 text-sm text-muted-foreground">
          Hyperliquid market data is disabled on this server. Enable the optional data integration to browse its outcomes.
        </CardContent></Card>
      )}
      {catalog?.enabled && (
        <div className="grid items-start gap-5 xl:grid-cols-[minmax(240px,1fr)_minmax(0,2fr)]">
          <Card>
            <CardHeader><CardTitle>{catalog.markets.length} listed outcomes</CardTitle></CardHeader>
            <CardContent className="max-h-[70vh] space-y-2 overflow-y-auto">
              {!catalog.markets.length && <p className="text-sm text-muted-foreground">No matching YES/NO outcomes.</p>}
              {catalog.markets.map((market) => (
                <button key={market.market_id} type="button" aria-pressed={selected?.market_id === market.market_id} onClick={() => setSelected(market)} className={`w-full rounded-lg border p-3 text-left text-sm transition-colors ${selected?.market_id === market.market_id ? "border-primary bg-primary/5" : "border-border hover:bg-muted/50"}`}>
                  <span className="block font-medium">{market.title}</span>
                  <span className="mt-1 block text-xs text-muted-foreground">Outcome {market.market_id}</span>
                </button>
              ))}
            </CardContent>
          </Card>
          {selected ? <SelectedOutcome key={selected.market_id} market={selected} /> : (
            <Card><CardContent className="py-14 text-center text-sm text-muted-foreground">Select an outcome to inspect its YES and NO books.</CardContent></Card>
          )}
        </div>
      )}
    </>
  )
}

/** Poll only the selected market, clearing expired data on request failure. */
function SelectedOutcome({ market }: { market: HyperliquidMarket }) {
  const [snapshot, setSnapshot] = useState<HyperliquidBooks | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout>
    async function load() {
      try {
        const result = await apiClient.hyperliquidBooks(market.market_id, controller.signal)
        if (controller.signal.aborted) return
        setSnapshot(result)
        setError(null)
      } catch (reason) {
        if (controller.signal.aborted) return
        setSnapshot(null)
        setError(reason instanceof Error ? reason.message : "Outcome books unavailable")
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(load, 5_000)
      }
    }
    void load()
    return () => { controller.abort(); clearTimeout(timer) }
  }, [market.market_id])

  return (
    <div className="min-w-0 space-y-4">
      <Card>
        <CardHeader><CardTitle>{market.title}</CardTitle></CardHeader>
        <CardContent className="space-y-3 text-xs text-muted-foreground">
          <p>Metadata deadline: {market.closes_at ? formatTimestamp(market.closes_at) : "Not supplied"}</p>
          <p>Native snapshots refresh every 5 seconds. Prices are USDC per token; quantities are tokens.</p>
          {market.rules && <details><summary className="cursor-pointer">Resolution rules</summary><p className="mt-3 whitespace-pre-line leading-relaxed">{market.rules}</p>{market.source && <p className="mt-3">Source: {market.source}</p>}</details>}
        </CardContent>
      </Card>
      {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
      {!snapshot && !error && <p role="status" className="text-sm text-muted-foreground">Loading books…</p>}
      {snapshot && (
        <>
          <div className="grid gap-4 lg:grid-cols-2 xl:grid-cols-1 2xl:grid-cols-2">
            {snapshot.books.map((book) => <OutcomeBook key={book.contract_id} book={book} />)}
          </div>
          <p className="text-xs text-muted-foreground">Fetched {formatTimestamp(snapshot.observed_at)} · The two books can have different venue timestamps.</p>
        </>
      )}
    </div>
  )
}

/** Show separate native bid and ask depth, without deriving either outcome's price. */
function OutcomeBook({ book }: { book: HyperliquidBook }) {
  return (
    <Card>
      <CardHeader><CardTitle>{book.outcome}</CardTitle><p className="text-xs text-muted-foreground">Venue time: {book.source_at ? formatTimestamp(book.source_at) : "Unavailable"}</p></CardHeader>
      <CardContent className="grid grid-cols-2 gap-4">
        {(["bids", "asks"] as const).map((side) => (
          <div key={side} className="min-w-0">
            <h4 className={`mb-2 text-xs font-semibold ${side === "bids" ? "text-emerald-700 dark:text-emerald-300" : "text-rose-700 dark:text-rose-300"}`}>{side === "bids" ? "Bids" : "Asks"}</h4>
            <table className="w-full text-right text-xs tabular-nums" aria-label={`${book.outcome} ${side}`}>
              <thead><tr className="text-muted-foreground"><th className="pb-2 text-left font-normal" scope="col">Price</th><th className="pb-2 font-normal" scope="col">Tokens</th></tr></thead>
              <tbody>
                {book[side].slice(0, 10).map(([price, size]) => <tr key={price} className="border-t border-border/50"><td className="py-1.5 text-left">{Number(price).toFixed(5)}</td><td className="py-1.5">{Number(size).toLocaleString("en-US", { maximumFractionDigits: 2 })}</td></tr>)}
                {!book[side].length && <tr><td colSpan={2} className="py-3 text-left text-muted-foreground">No levels</td></tr>}
              </tbody>
            </table>
          </div>
        ))}
      </CardContent>
    </Card>
  )
}
