import { useEffect, useMemo, useRef, useState } from "react"
import {
  BaselineSeries,
  ColorType,
  CrosshairMode,
  LineStyle,
  LineType,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts"

import { Button } from "@/components/ui/button"
import { formatPrice, formatTimestamp } from "@/lib/formatters"
import type { PnlPoint } from "@/types/pnl"

function usd(value: number) {
  return `${value < 0 ? "−" : ""}$${formatPrice(Math.abs(value))}`
}

/** Keep the last cumulative balance per second, as required by TradingView. */
export function chartPoints(series: PnlPoint[]) {
  const points = new Map<number, number>()
  for (const point of [...series].sort((a, b) => Date.parse(a.observed_at) - Date.parse(b.observed_at))) {
    const time = Math.floor(Date.parse(point.observed_at) / 1_000)
    const value = Number(point.net_pnl_usd)
    if (Number.isFinite(time) && Number.isFinite(value)) points.set(time, value)
  }
  return [...points].map(([time, value]) => ({ time: time as UTCTimestamp, value }))
}

/** Show execution-based cumulative PnL with TradingView zoom, pan, and crosshair. */
export function PnlChart({ series }: { series: PnlPoint[] }) {
  const container = useRef<HTMLDivElement>(null)
  const chart = useRef<IChartApi | null>(null)
  const line = useRef<ISeriesApi<"Baseline"> | null>(null)
  const fitted = useRef(false)
  const [hovered, setHovered] = useState<{ time: number; value: number } | null>(null)
  const data = useMemo(() => chartPoints(series), [series])
  const active = hovered ?? data.at(-1)

  useEffect(() => {
    if (!container.current) return
    const instance = createChart(container.current, {
      autoSize: true,
      height: 360,
      layout: { background: { type: ColorType.Solid, color: "transparent" }, attributionLogo: true },
      crosshair: { mode: CrosshairMode.Magnet },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.18, bottom: 0.15 } },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: true, rightOffset: 3 },
      localization: {
        priceFormatter: usd,
        timeFormatter: (time: Time) => formatTimestamp(new Date(Number(time) * 1_000)),
      },
      handleScroll: { vertTouchDrag: false },
    })
    const result = instance.addSeries(BaselineSeries, {
      baseValue: { type: "price", price: 0 },
      topLineColor: "#10b981", bottomLineColor: "#f43f5e",
      topFillColor1: "rgba(16,185,129,0.20)", topFillColor2: "rgba(16,185,129,0.02)",
      bottomFillColor1: "rgba(244,63,94,0.02)", bottomFillColor2: "rgba(244,63,94,0.20)",
      lineWidth: 2, lineType: LineType.WithSteps,
      priceFormat: { type: "price", precision: 4, minMove: 0.0001 },
      pointMarkersVisible: true,
      pointMarkersRadius: 3,
    })
    result.createPriceLine({ price: 0, color: "#94a3b8", lineWidth: 1, lineStyle: LineStyle.Dashed, axisLabelVisible: true, title: "Break-even" })
    function applyTheme() {
      const dark = document.documentElement.classList.contains("dark")
      instance.applyOptions({
        layout: { textColor: dark ? "#94a3b8" : "#475569" },
        grid: {
          vertLines: { visible: false },
          horzLines: { color: dark ? "#29303b" : "#e2e8f0", style: LineStyle.Dotted },
        },
      })
    }
    applyTheme()
    const theme = new MutationObserver(applyTheme)
    theme.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] })
    instance.subscribeCrosshairMove((event) => {
      const point = event.seriesData.get(result)
      setHovered(event.time !== undefined && point && "value" in point
        ? { time: Number(event.time), value: point.value }
        : null)
    })
    chart.current = instance
    line.current = result
    fitted.current = false
    return () => {
      theme.disconnect()
      instance.remove()
      chart.current = null
      line.current = null
    }
  }, [])

  useEffect(() => {
    line.current?.setData(data)
    setHovered(null)
    if (!fitted.current && data.length) {
      chart.current?.timeScale().fitContent()
      fitted.current = true
    }
  }, [data])

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
          <span className="numeric text-2xl font-semibold">{active ? usd(active.value) : "No history"}</span>
          <span className="text-xs text-muted-foreground">{active ? formatTimestamp(new Date(active.time * 1_000)) : "Executions and recorded gas will appear here."}</span>
        </div>
        <Button variant="outline" size="sm" onClick={() => chart.current?.timeScale().fitContent()}>Reset zoom</Button>
      </div>
      <div className="relative">
        <div ref={container} className="h-[360px] w-full" role="img" aria-label="Cumulative arbitrage PnL after trading fees and recorded gas" />
        {!data.length && <div className="pointer-events-none absolute inset-0 flex items-center justify-center text-sm text-muted-foreground">No recorded results yet.</div>}
      </div>
      <div className="mt-3 flex flex-wrap justify-between gap-2 text-[11px] text-muted-foreground">
        <span>Scroll to zoom · drag to pan · hover to inspect</span>
        <a href="https://www.tradingview.com/" target="_blank" rel="noreferrer">TradingView Lightweight Charts™ · Copyright © 2025 TradingView, Inc.</a>
      </div>
      {data.length === 1 && <p className="mt-3 text-xs text-muted-foreground">One recorded result. More executions will extend the curve.</p>}
      <details className="mt-4 text-xs text-muted-foreground">
        <summary className="cursor-pointer">View chart data</summary>
        <div className="mt-3 max-h-48 overflow-auto">
          <table className="w-full text-left">
            <caption className="sr-only">Cumulative net result at each recorded execution or gas charge</caption>
            <thead><tr><th className="py-2">Time</th><th className="py-2 text-right">Net PnL (USD)</th></tr></thead>
            <tbody>{series.map((point, index) => <tr key={`${point.observed_at}-${index}`}><td className="py-1">{formatTimestamp(point.observed_at)}</td><td className="numeric text-right">{usd(Number(point.net_pnl_usd))}</td></tr>)}</tbody>
          </table>
        </div>
      </details>
    </div>
  )
}
