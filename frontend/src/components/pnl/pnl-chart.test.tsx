import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { beforeEach, expect, test, vi } from "vitest"

import { chartPoints, PnlChart } from "@/components/pnl/pnl-chart"

const api = vi.hoisted(() => ({
  setData: vi.fn(), fitContent: vi.fn(), remove: vi.fn(),
  applyOptions: vi.fn(), createPriceLine: vi.fn(), subscribeCrosshairMove: vi.fn(),
}))

vi.mock("lightweight-charts", () => ({
  BaselineSeries: {}, ColorType: { Solid: "solid" }, CrosshairMode: { Magnet: 1 },
  LineStyle: { Dashed: 2, Dotted: 1 }, LineType: { WithSteps: 1 },
  createChart: () => ({
    addSeries: () => ({ setData: api.setData, createPriceLine: api.createPriceLine }),
    timeScale: () => ({ fitContent: api.fitContent }),
    remove: api.remove, applyOptions: api.applyOptions, subscribeCrosshairMove: api.subscribeCrosshairMove,
  }),
}))

beforeEach(() => vi.clearAllMocks())

test("keeps the final cumulative balance within each second in chronological order", () => {
  expect(chartPoints([
    { observed_at: "2026-08-17T08:00:01.900Z", net_pnl_usd: "2" },
    { observed_at: "2026-08-17T08:00:00Z", net_pnl_usd: "-0.1" },
    { observed_at: "2026-08-17T08:00:01.100Z", net_pnl_usd: "1" },
    { observed_at: "invalid", net_pnl_usd: "3" },
    { observed_at: "2026-08-17T08:00:02Z", net_pnl_usd: "NaN" },
  ])).toEqual([
    { time: Date.parse("2026-08-17T08:00:00Z") / 1000, value: -0.1 },
    { time: Date.parse("2026-08-17T08:00:01Z") / 1000, value: 2 },
  ])
})

test("preserves the user's zoom on refresh and removes the chart on unmount", async () => {
  const user = userEvent.setup()
  const series = [{ observed_at: "2026-08-17T08:00:00Z", net_pnl_usd: "-0.05" }]
  const { rerender, unmount } = render(<PnlChart series={series} />)
  expect(api.fitContent).toHaveBeenCalledTimes(1)
  expect(screen.getByText(/One recorded result/)).toBeInTheDocument()

  const updated = [...series, { observed_at: "2026-08-17T09:00:00Z", net_pnl_usd: "1.25" }]
  rerender(<PnlChart series={updated} />)
  expect(api.setData).toHaveBeenLastCalledWith(chartPoints(updated))
  expect(api.fitContent).toHaveBeenCalledTimes(1)
  expect(screen.queryByText(/One recorded result/)).not.toBeInTheDocument()
  await user.click(screen.getByRole("button", { name: "Reset zoom" }))
  expect(api.fitContent).toHaveBeenCalledTimes(2)
  unmount()
  expect(api.remove).toHaveBeenCalledTimes(1)
})

test("keeps an empty chart empty instead of inventing a profit point", () => {
  render(<PnlChart series={[]} />)
  expect(api.setData).toHaveBeenCalledWith([])
  expect(api.fitContent).not.toHaveBeenCalled()
  expect(screen.getByText("No recorded results yet.")).toBeInTheDocument()
})
