import { render, screen } from "@testing-library/react"
import { describe, expect, test, vi } from "vitest"

import { VenueHealthPage } from "@/components/venue-health/venue-health-page"

vi.mock("@/features/venue-health/use-venue-health", () => ({
  useVenueHealth: () => ({
    loading: false,
    error: null,
    report: {
      generated_at: "2026-08-19T09:20:00Z",
      overall_status: "degraded",
      venues: [
        {
          venue_id: "POLYMARKET",
          status: "unavailable",
          checked_at: "2026-08-19T09:20:00Z",
          latency_ms: 320.4,
          source: "CLOB API + official status",
          message: "Trading outage",
          error_type: "OfficialMajorOutage",
          http_status: 200,
          retryable: true,
        },
        {
          venue_id: "PREDICT",
          status: "operational",
          checked_at: "2026-08-19T09:20:00Z",
          latency_ms: 80,
          source: "Predict Markets API",
          message: "Markets API reachable",
          error_type: null,
          http_status: 200,
          retryable: false,
        },
      ],
    },
  }),
}))

describe("VenueHealthPage", () => {
  test("renders venue status, latency, and existing logos", () => {
    const { container } = render(<VenueHealthPage />)

    expect(screen.getByText("Overall · Degraded")).toBeInTheDocument()
    expect(screen.getByText("Trading outage")).toBeInTheDocument()
    expect(screen.getByText("320 ms")).toBeInTheDocument()
    expect(screen.getByText("OfficialMajorOutage")).toBeInTheDocument()
    expect(screen.getAllByText("200")).toHaveLength(2)
    expect(screen.getByText("Yes")).toBeInTheDocument()
    expect(
      container.querySelector('img[src="/venues/polymarket.svg"]'),
    ).toBeInTheDocument()
    expect(
      container.querySelector('img[src="/venues/predict.png"]'),
    ).toBeInTheDocument()
  })
})
