import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { describe, expect, test, vi } from "vitest"

import { TradingControl } from "@/components/runtime/trading-control"
import type { TradingRunController } from "@/features/trading/use-trading-run"
import type { TradingRunStatus } from "@/types/trading"

function controller(
  status: TradingRunStatus | null,
  overrides: Partial<TradingRunController> = {},
): TradingRunController {
  return {
    run: status
      ? {
          id: "run-1",
          status,
          started_at: "2026-07-29T12:00:00Z",
          finished_at: null,
          error: null,
          short_market_keys: [],
        }
      : null,
    requestState: "success",
    error: null,
    enable: vi.fn().mockResolvedValue(undefined),
    disable: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

describe("TradingControl", () => {
  test("enables live trading only after confirmation and key entry", async () => {
    const user = userEvent.setup()
    const value = controller(null)
    render(
      <TradingControl
        controller={value}
        signalSettings={{ min_net_edge: 0.015, cost_buffer: 0.005 }}
      />,
    )

    await user.click(screen.getByRole("button", { name: /enable trading/i }))
    expect(screen.getByRole("alertdialog")).toHaveClass(
      "max-w-4xl",
      "overflow-y-auto",
    )
    const confirm = screen.getByRole("button", {
      name: "Enable live trading",
      hidden: false,
    })
    expect(confirm).toBeDisabled()

    await user.type(screen.getByLabelText("Trading API key"), "secret")
    expect(confirm).toBeDisabled()
    expect(
      screen.getByRole("button", { name: "Enable despite degraded" }),
    ).toBeDisabled()
    expect(screen.getByText(/Real funds at risk/)).toBeVisible()
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this uses real funds/,
      }),
    )
    await user.click(confirm)

    expect(value.enable).toHaveBeenCalledWith("secret", {
      max_arbitrages: 1,
      max_concurrent_arbitrages: 1,
      polymarket_max_notional: 1,
      limitless_max_notional: 1,
      predict_max_notional: 1,
      predict_limit_slippage_ticks: 0,
      predict_use_edge_budget: false,
      min_net_edge: 0.015,
      cost_buffer: 0.005,
      max_recovery_loss: 1,
      short_market_keys: [],
    })
  })

  test("passes the selected execution count, venue budgets and profit requirements", async () => {
    const user = userEvent.setup()
    const value = controller(null)
    render(
      <TradingControl
        controller={value}
        shortMarketKeys={["regular:selected"]}
      />,
    )

    await user.click(screen.getByRole("button", { name: /enable trading/i }))
    expect(screen.getByText(/1 short market is selected/i)).toBeInTheDocument()
    await user.clear(screen.getByLabelText("Arbitrages before stop"))
    await user.type(screen.getByLabelText("Arbitrages before stop"), "3")
    await user.clear(screen.getByLabelText("Concurrent arbitrages"))
    await user.type(screen.getByLabelText("Concurrent arbitrages"), "2")
    await user.clear(screen.getByLabelText("Polymarket max (USDC)"))
    await user.type(screen.getByLabelText("Polymarket max (USDC)"), "10")
    await user.clear(screen.getByLabelText("Limitless max (USDC)"))
    await user.type(screen.getByLabelText("Limitless max (USDC)"), "10")
    await user.clear(screen.getByLabelText("Predict max (USDT)"))
    await user.type(screen.getByLabelText("Predict max (USDT)"), "10")
    await user.clear(screen.getByLabelText("Recovery max loss (USD)"))
    await user.type(screen.getByLabelText("Recovery max loss (USD)"), "0.5")
    await user.clear(screen.getByLabelText("Minimum net edge (USD/contract)"))
    await user.type(screen.getByLabelText("Minimum net edge (USD/contract)"), "0.015")
    await user.clear(screen.getByLabelText("Cost buffer (USD/contract)"))
    await user.type(screen.getByLabelText("Cost buffer (USD/contract)"), "0.005")
    await user.type(screen.getByLabelText("Trading API key"), "secret")
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this uses real funds/,
      }),
    )
    await user.click(
      screen.getByRole("button", {
        name: "Enable live trading",
        hidden: false,
      }),
    )

    expect(value.enable).toHaveBeenCalledWith("secret", {
      max_arbitrages: 3,
      max_concurrent_arbitrages: 2,
      polymarket_max_notional: 10,
      limitless_max_notional: 10,
      predict_max_notional: 10,
      predict_limit_slippage_ticks: 0,
      predict_use_edge_budget: false,
      min_net_edge: 0.015,
      cost_buffer: 0.005,
      max_recovery_loss: 0.5,
      short_market_keys: ["regular:selected"],
    })
  })

  test("starts without Predict slippage or experimental pricing controls", async () => {
    const user = userEvent.setup()
    const value = controller(null)
    render(<TradingControl controller={value} />)
    await user.click(screen.getByRole("button", { name: /enable trading/i }))
    expect(screen.queryByLabelText("Predict slippage (ticks)")).not.toBeInTheDocument()
    expect(screen.queryByRole("checkbox", {
      name: /Predict: use available edge/i,
    })).not.toBeInTheDocument()
    await user.type(screen.getByLabelText("Trading API key"), "secret")
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this uses real funds/,
      }),
    )
    await user.click(
      screen.getByRole("button", { name: "Enable live trading" }),
    )
    expect(value.enable).toHaveBeenCalledWith(
      "secret",
      expect.objectContaining({
        predict_use_edge_budget: false,
        predict_limit_slippage_ticks: 0,
        max_arbitrages: 1,
      }),
    )
  })

  test.each([
    ["Minimum net edge (USD/contract)", ""],
    ["Minimum net edge (USD/contract)", "-0.01"],
    ["Minimum net edge (USD/contract)", "0.201"],
    ["Cost buffer (USD/contract)", ""],
    ["Cost buffer (USD/contract)", "-0.01"],
    ["Cost buffer (USD/contract)", "0.051"],
  ])("blocks both start actions for invalid %s: %s", async (label, invalid) => {
    const user = userEvent.setup()
    const value = controller(null)
    render(<TradingControl controller={value} />)
    await user.click(screen.getByRole("button", { name: "Enable trading" }))
    await user.type(screen.getByLabelText("Trading API key"), "secret")
    await user.click(screen.getByRole("checkbox", { name: /I understand this uses real funds/ }))
    expect(screen.getByRole("button", { name: "Enable live trading" })).toBeEnabled()
    await user.clear(screen.getByLabelText(label))
    if (invalid) await user.type(screen.getByLabelText(label), invalid)
    expect(screen.getByRole("button", { name: "Enable live trading" })).toBeDisabled()
    expect(screen.getByRole("button", { name: "Enable despite degraded" })).toBeDisabled()
    expect(value.enable).not.toHaveBeenCalled()
  })

  test("uses refreshed thresholds without overwriting edits and discards cancelled changes", async () => {
    const user = userEvent.setup()
    const value = controller(null)
    const view = render(<TradingControl controller={value} />)
    await user.click(screen.getByRole("button", { name: "Enable trading" }))
    view.rerender(<TradingControl controller={value} signalSettings={{ min_net_edge: 0.01, cost_buffer: 0.002 }} />)
    expect(screen.getByLabelText("Minimum net edge (USD/contract)")).toHaveValue(0.01)
    expect(screen.getByLabelText("Cost buffer (USD/contract)")).toHaveValue(0.002)
    await user.clear(screen.getByLabelText("Minimum net edge (USD/contract)"))
    await user.type(screen.getByLabelText("Minimum net edge (USD/contract)"), "0.02")
    view.rerender(<TradingControl controller={value} signalSettings={{ min_net_edge: 0.01, cost_buffer: 0.003 }} />)
    expect(screen.getByLabelText("Minimum net edge (USD/contract)")).toHaveValue(0.02)
    await user.click(screen.getByRole("button", { name: "Cancel" }))
    expect(value.enable).not.toHaveBeenCalled()
    await user.click(screen.getByRole("button", { name: "Enable trading" }))
    expect(screen.getByLabelText("Minimum net edge (USD/contract)")).toHaveValue(0.01)
    expect(screen.getByLabelText("Cost buffer (USD/contract)")).toHaveValue(0.003)
  })

  test("can explicitly start while venue health is degraded", async () => {
    const user = userEvent.setup()
    const value = controller(null)
    render(<TradingControl controller={value} />)

    await user.click(screen.getByRole("button", { name: /enable trading/i }))
    await user.type(screen.getByLabelText("Trading API key"), "secret")
    expect(
      screen.getByRole("button", { name: "Enable despite degraded" }),
    ).toBeDisabled()
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this uses real funds/,
      }),
    )
    await user.click(
      screen.getByRole("button", { name: "Enable despite degraded" }),
    )

    expect(value.enable).toHaveBeenCalledWith(
      "secret",
      expect.objectContaining({ allow_degraded_venues: true }),
    )
  })

  test("disables an active run only after confirmation", async () => {
    const user = userEvent.setup()
    const value = controller("running")
    render(<TradingControl controller={value} />)

    await user.click(screen.getByRole("button", { name: /disable trading/i }))
    await user.click(
      screen.getByRole("button", {
        name: "Disable trading",
        hidden: false,
      }),
    )

    expect(value.disable).toHaveBeenCalledOnce()
  })

  test("requires a fresh risk acknowledgement and key after cancelling", async () => {
    const user = userEvent.setup()
    const value = controller(null)
    render(<TradingControl controller={value} />)
    await user.click(screen.getByRole("button", { name: /enable trading/i }))
    await user.type(screen.getByLabelText("Trading API key"), "secret")
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this uses real funds/,
      }),
    )
    await user.click(screen.getByRole("button", { name: "Cancel" }))
    await user.click(screen.getByRole("button", { name: /enable trading/i }))
    expect(
      screen.getByRole("checkbox", {
        name: /I understand this uses real funds/,
      }),
    ).not.toBeChecked()
    expect(screen.getByLabelText("Trading API key")).toHaveValue("")
    expect(
      screen.getByRole("button", { name: "Enable live trading" }),
    ).toBeDisabled()
    expect(value.enable).not.toHaveBeenCalled()
  })

  test("shows collateral preparation as an active run", () => {
    render(<TradingControl controller={controller("preparing")} />)

    expect(screen.getByText("Preparing short collateral")).toBeVisible()
    expect(
      screen.getByRole("button", { name: /disable trading/i }),
    ).toBeEnabled()
  })

  test("shows a failed run error returned after startup", () => {
    const value = controller("failed", {
      run: {
        id: "run-1",
        status: "failed",
        started_at: "2026-07-29T12:00:00Z",
        finished_at: "2026-07-29T12:00:01Z",
        error: "Invalid journal frame during read",
        short_market_keys: [],
      },
    })

    render(<TradingControl controller={value} />)

    expect(screen.getByText("Invalid journal frame during read")).toBeVisible()
  })
})
