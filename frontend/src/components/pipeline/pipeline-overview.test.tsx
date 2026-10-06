import { render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { describe, expect, test } from "vitest"
import { PipelineOverview } from "@/components/pipeline/pipeline-overview"
import {
  LatencyOverview,
  metricDuration,
} from "@/components/pipeline/latency-overview"
import { pipelineMetrics } from "@/lib/prometheus"

describe("Pipeline and latency summaries", () => {
  test("distinguishes an idle runtime and absent telemetry from a failed service", () => {
    render(
      <PipelineOverview
        runtime={{
          running: true,
          regular_markets: [],
          input_buffer_size: 0,
          trading_enabled: false,
        }}
        pipeline={pipelineMetrics("")}
        unavailable={false}
      />,
    )
    expect(screen.getByText("Waiting for events")).toBeInTheDocument()
    expect(screen.getAllByText("No samples")).toHaveLength(3)
    expect(
      screen.getByRole("link", { name: /Explore events/ }),
    ).toHaveAttribute("href", "/events")
    expect(screen.queryByText("Unavailable")).not.toBeInTheDocument()
  })

  test("marks stale telemetry unavailable instead of claiming a healthy feed", () => {
    render(
      <PipelineOverview
        runtime={{ running: true, regular_markets: [] }}
        pipeline={pipelineMetrics("")}
        unavailable
      />,
    )
    expect(screen.getByText("Unavailable")).toBeInTheDocument()
    expect(screen.getAllByText("Telemetry unavailable")).toHaveLength(3)
    expect(
      screen.queryByText(/waiting for its first event/i),
    ).not.toBeInTheDocument()
  })

  test("changes the percentile comparison while keeping overview cards explicitly p95", async () => {
    const user = userEvent.setup()
    const pipeline = pipelineMetrics("")
    pipeline.stages = [
      {
        id: "submit",
        metric: "arbitrage_stage_seconds",
        label: "submit",
        labels: { stage: "opportunity_to_both_submits" },
        p50: 0.012,
        p95: 0.048,
        p99: 0.096,
        p999: null,
        maximumLowerBound: 0.05,
        maximumUpperBound: 0.1,
        count: 20,
      },
    ]
    render(<LatencyOverview pipeline={pipeline} attempt={null} />)
    expect(screen.getByText("48 ms")).toBeInTheDocument()
    await user.click(screen.getByRole("button", { name: "p99" }))
    expect(screen.getByText("96 ms")).toBeInTheDocument()
    expect(
      screen.getByRole("button", { name: "p99" }),
    ).toHaveAttribute("aria-pressed", "true")
    expect(
      within(screen.getByRole("region", { name: "Latency summary" })).getByText(
        "Both submits · p95",
      ),
    ).toBeInTheDocument()
  })

  test("preserves zero and sub-millisecond observations without inventing missing values", () => {
    expect(metricDuration(0)).toBe("0 ms")
    expect(metricDuration(0.000125)).toBe("0.125 ms")
    expect(metricDuration(null)).toBe("No samples")
    expect(metricDuration(Number.NaN)).toBe("No samples")
  })
})
