import { render, screen } from "@testing-library/react"
import { describe, expect, test, vi } from "vitest"

import { SignalsPage } from "@/components/signals/signals-page"

const state = vi.hoisted(() => ({
  updateSignalSettings: vi.fn().mockResolvedValue(undefined),
}))

vi.mock("@/features/arbitrage/arbitrage-stream-provider", () => ({
  useArbitrageStream: () => ({
    connectionStatus: "connected",
    opportunities: [],
  }),
}))

vi.mock("@/features/runtime/use-runtime", () => ({
  useRuntime: () => ({
    runtime: {
      running: true,
      trading_enabled: false,
      signal_settings: { min_net_edge: 0, cost_buffer: 0 },
    },
    requestState: "success",
    error: null,
    updateSignalSettings: state.updateSignalSettings,
  }),
}))

describe("SignalsPage", () => {
  test("keeps opportunities read-only after moving thresholds to trading setup", () => {
    render(<SignalsPage />)
    expect(screen.getByRole("combobox", { name: "Filter by opportunity type" })).toBeVisible()
    expect(screen.queryByLabelText("Minimum net edge (USD/contract)")).not.toBeInTheDocument()
    expect(screen.queryByLabelText("Cost buffer (USD/contract)")).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Apply signal settings" })).not.toBeInTheDocument()
    expect(state.updateSignalSettings).not.toHaveBeenCalled()
  })
})
