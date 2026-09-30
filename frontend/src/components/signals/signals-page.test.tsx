import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
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

vi.mock("@/features/markets/use-market-matches", () => ({
  useMarketMatches: () => ({
    matches: [],
    loading: false,
    error: null,
    updatedAt: null,
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
  test("applies fee-aware thresholds to signal detection", async () => {
    const user = userEvent.setup()
    render(<SignalsPage />)

    await user.clear(screen.getByLabelText("Minimum net edge (USD/contract)"))
    await user.type(
      screen.getByLabelText("Minimum net edge (USD/contract)"),
      "0.015",
    )
    await user.clear(screen.getByLabelText("Cost buffer (USD/contract)"))
    await user.type(
      screen.getByLabelText("Cost buffer (USD/contract)"),
      "0.005",
    )
    await user.click(screen.getByRole("button", { name: "Apply signal settings" }))

    expect(state.updateSignalSettings).toHaveBeenCalledWith({
      min_net_edge: 0.015,
      cost_buffer: 0.005,
    })
  })
})
