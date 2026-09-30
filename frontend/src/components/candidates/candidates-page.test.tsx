import { fireEvent, render, screen } from "@testing-library/react"
import { describe, expect, test, vi } from "vitest"

import { CandidatesPage } from "@/components/candidates/candidates-page"
import type { AggDiscoveryView } from "@/features/agg/use-agg-candidates"

const state = vi.hoisted(() => ({
  view: null as AggDiscoveryView | null,
  filters: null as { searchText?: string } | null,
}))

vi.mock("@/features/agg/use-agg-candidates", () => ({
  useArbitrageCandidates: (
    filters: { searchText?: string },
    view: AggDiscoveryView,
  ) => {
    state.view = view
    state.filters = filters
    return {
      candidates: [],
      loading: false,
      error: null,
      updatedAt: null,
    }
  },
}))

describe("CandidatesPage", () => {
  test("defaults to Fed markets and can switch to live edges", () => {
    render(<CandidatesPage />)

    expect(state.view).toBe("markets")
    expect(state.filters).toEqual({ searchText: "Fed" })
    expect(screen.getByText("No matching events available")).toBeInTheDocument()

    fireEvent.click(screen.getByRole("button", { name: "Live edges" }))

    expect(state.view).toBe("opportunities")
    expect(screen.getByText("No live edges available")).toBeInTheDocument()
  })
})
