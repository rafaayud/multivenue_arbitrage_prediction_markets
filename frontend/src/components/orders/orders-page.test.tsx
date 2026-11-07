import { render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { beforeEach, describe, expect, test, vi } from "vitest"

import { OrdersPage } from "@/components/orders/orders-page"
import { usePnl } from "@/features/pnl/use-pnl"
import type { ExecutionActivitySnapshot } from "@/types/execution"

const snapshot: ExecutionActivitySnapshot = {
  type: "execution_activity_snapshot",
  generatedAt: "2026-08-17T09:47:07Z",
  tradingFeesUsd: 0.00786,
  gasUsd: 0.0042,
  orders: [
    {
      status: "rejected",
      contractId: "predict:1418651:no",
      side: "sell",
      quantity: 5,
      orderType: "limit",
      clientOrderId: "primary-order",
      orderId: "primary-venue-order",
      limitPrice: 0.04,
      filledQuantity: 0,
      averagePrice: null,
      createdAt: "2026-08-17T09:47:02Z",
      updatedAt: "2026-08-17T09:47:03Z",
    },
  ],
  trades: [],
  positions: [],
  journals: [
    {
      executionId: "execution-1",
      monitorType: "cycle",
      monitorKey: "cycle:BTC:3600",
      marketLabel: null,
      underlying: "BTC",
      intervalSeconds: 3600,
      status: "recovered",
      leg1: {
        venueId: "PREDICT",
        contractId: "predict:1418651:no",
        side: "sell",
        quantity: 5,
        limitPrice: 0.04,
        clientOrderId: "primary-order",
        orderId: "primary-venue-order",
        filledQuantity: 0,
        averageFillPrice: null,
        feeAmount: null,
        feeCurrency: null,
        feeSettlementAmount: null,
        feeSettlementCurrency: null,
      },
      leg2: {
        venueId: "POLYMARKET",
        contractId: "polymarket:condition:token",
        side: "sell",
        quantity: 5,
        limitPrice: 0.97,
        clientOrderId: "hedge-order",
        orderId: "hedge-venue-order",
        filledQuantity: 5,
        averageFillPrice: 0.98,
        feeAmount: 0.00686,
        feeCurrency: "USDC",
        feeSettlementAmount: 0.00686,
        feeSettlementCurrency: "USD",
      },
      residualQuantity: 0,
      grossLockedPnlUsd: -0.05,
      totalFeeSettlementCostUsd: 0.00786,
      netLockedPnlUsd: -0.05786,
      manualResolution: null,
      latencyTrace: null,
      portfolioId: null,
      strategyId: null,
      lastError: "primary leg did not fill",
      createdAt: "2026-08-17T09:47:02.140Z",
      updatedAt: "2026-08-17T09:47:06.735Z",
    },
  ],
  recoveries: [
    {
      recoveryId: "recovery-1",
      executionId: "execution-1",
      route: "complete_missing_leg",
      venueId: "PREDICT",
      contractId: "predict:1418651:no",
      side: "sell",
      quantity: 5,
      filledQuantity: 5,
      limitPrice: 0.01,
      averagePrice: 0.01,
      sourceContractId: "polymarket:condition:token",
      sourceSide: "sell",
      sourcePrice: 0.98,
      sourceFeeAmount: 0.00686,
      sourceFeeCurrency: "USD",
      estimatedVwap: 0.01,
      estimatedRecoveryFeeAmount: 0.001,
      estimatedRecoveryFeeCurrency: "USD",
      estimatedGrossResult: -0.05,
      estimatedNetResult: -0.05786,
      recoveryFeeAmount: 0.001,
      recoveryFeeCurrency: "USD",
      actualGrossResult: -0.05,
      actualNetResult: -0.05786,
      portfolioId: null,
      strategyId: null,
      status: "resolved",
      attempts: 1,
      clientOrderId: "recovery-order",
      orderId: "recovery-venue-order",
      lastError: null,
      createdAt: "2026-08-17T09:47:03.032Z",
      updatedAt: "2026-08-17T09:47:06.735Z",
    },
  ],
}

snapshot.journals.push({
  ...snapshot.journals[0]!,
  executionId: "execution-needs-review",
  status: "needs_review",
  leg1: {
    ...snapshot.journals[0]!.leg1,
    filledQuantity: 0,
  },
  leg2: {
    ...snapshot.journals[0]!.leg2,
    filledQuantity: 5,
  },
  residualQuantity: 5,
  grossLockedPnlUsd: null,
  totalFeeSettlementCostUsd: null,
  netLockedPnlUsd: null,
  lastError: "automatic recovery needs operator review",
})

const completeExecution = vi.hoisted(() => vi.fn().mockResolvedValue(undefined))

const pnlState = {
  pnl: null as { terminal_executions: { net_pnl_usd: string; fees_usd: string; gas_usd: string | null } } | null,
  error: null as string | null,
}

vi.mock("@/features/pnl/use-pnl", () => ({
  usePnl: vi.fn(() => pnlState),
}))

beforeEach(() => {
  pnlState.pnl = { terminal_executions: { net_pnl_usd: "-0.05786", fees_usd: "0.00786", gas_usd: "0.0042" } }
  pnlState.error = null
})

vi.mock("@/lib/api-client", () => ({
  apiClient: { completeExecution },
}))

vi.mock("@/features/execution/execution-activity-provider", () => ({
  useExecutionActivity: () => ({
    snapshot,
    connectionStatus: "connected",
    connectionError: null,
    lastReceivedAt: new Date("2026-08-17T09:47:07Z"),
    authenticate: vi.fn(),
  }),
}))

const runtimeState = {
  runtime: {
    regular_markets: [] as Array<{ monitor_key: string; markets: Array<{ title: string }> }>,
  },
}

vi.mock("@/features/runtime/use-runtime", () => ({
  useRuntime: () => runtimeState,
}))

describe("OrdersPage", () => {
  test("keeps the recorded market name after monitoring stops and the page reloads", () => {
    const original = snapshot.journals[0]!
    const label = "Will the regular event happen?"
    snapshot.journals[0] = {
      ...original,
      monitorType: "regular",
      monitorKey: "regular:event",
      marketLabel: label,
      underlying: null,
      intervalSeconds: null,
    }
    runtimeState.runtime.regular_markets = [
      { monitor_key: "regular:event", markets: [{ title: "Current catalog title" }] },
    ]
    try {
      const view = render(<OrdersPage />)
      expect(screen.getByText(label)).toHaveAttribute("title", label)

      runtimeState.runtime.regular_markets = []
      view.rerender(<OrdersPage />)
      expect(screen.getByText(label)).toBeInTheDocument()

      view.unmount()
      render(<OrdersPage />)
      expect(screen.getByText(label)).toBeInTheDocument()
    } finally {
      snapshot.journals[0] = original
      runtimeState.runtime.regular_markets = []
    }
  })

  test("uses all-time PnL totals instead of summing the limited activity feed", () => {
    pnlState.pnl!.terminal_executions = {
      net_pnl_usd: "3.258711242961635625", fees_usd: "2.986055744664911056", gas_usd: "0.1736243193732856",
    }
    render(<OrdersPage />)

    expect(usePnl).toHaveBeenLastCalledWith("bot_ledger", "ALL")
    const card = (label: string) => within(screen.getByText(label).parentElement!)
    expect(card("Closed execution net PnL").getByText("+$3.2587")).toBeInTheDocument()
    expect(card("Trading fees").getByText("$2.9861")).toBeInTheDocument()
    expect(card("Gas (inventory ops)").getByText("$0.1736")).toBeInTheDocument()
    expect(screen.getByText(/subtract recorded gas to match the PnL tab/)).toBeInTheDocument()
  })

  test("keeps all-time totals unavailable instead of falling back to recent activity", () => {
    pnlState.pnl = null
    pnlState.error = "Unavailable"
    render(<OrdersPage />)

    expect(screen.getByRole("alert")).toHaveTextContent("Unavailable")
    for (const label of ["Closed execution net PnL", "Trading fees", "Gas (inventory ops)"]) {
      expect(within(screen.getByText(label).parentElement!).getByText("—")).toBeInTheDocument()
    }
  })

  test("explains a recovered execution beside its original legs", () => {
    render(<OrdersPage />)

    expect(screen.getByText("Recovered after a failed leg")).toBeInTheDocument()
    expect(screen.getByText("Residual recovery result")).toBeInTheDocument()
    expect(screen.getByText(/excludes quantity already paired/)).toBeInTheDocument()
    expect(screen.getAllByText("primary leg did not fill")).toHaveLength(2)
    expect(screen.getByText("-$0.0579 net")).toBeInTheDocument()
    expect(screen.getByText("Closed execution net PnL")).toBeInTheDocument()
    expect(screen.getByText("Trading fees")).toBeInTheDocument()
    expect(screen.getByText("Gas (inventory ops)")).toBeInTheDocument()
    expect(screen.getByText("$0.0042")).toBeInTheDocument()
    expect(screen.getAllByText("-$0.0579")).toHaveLength(2)
    expect(screen.getByText("Exposure neutralized")).toBeInTheDocument()
    expect(screen.getAllByText("$0.0079")).toHaveLength(3)
    expect(screen.getByText("4.59s")).toBeInTheDocument()
  })

  test("records actual settlement data without choosing execution identity", async () => {
    const user = userEvent.setup()
    render(<OrdersPage />)

    await user.click(screen.getByRole("button", { name: "Resolve manually" }))
    expect(
      screen.getByText(/will not send an order to POLYMARKET/i),
    ).toBeInTheDocument()
    expect(screen.getByText("polymarket:condition:token")).toBeInTheDocument()

    await user.type(
      screen.getByLabelText("Transaction, claim, or note (optional)"),
      "claim-0x123",
    )
    await user.click(screen.getByRole("button", { name: "Record resolution" }))

    expect(completeExecution).toHaveBeenCalledWith(
      "execution-needs-review",
      expect.objectContaining({
        method: "settlement",
        price: 1,
        feeAmountUsd: 0,
        externalReference: "claim-0x123",
        executedAt: expect.any(String),
      }),
    )
  })

  test("shows the economics recorded for a manually resolved execution", () => {
    Object.assign(snapshot.journals[1]!, {
      status: "completed",
      residualQuantity: 0,
      grossLockedPnlUsd: 12.70124,
      totalFeeSettlementCostUsd: 0.01,
      netLockedPnlUsd: 12.69124,
      lastError: null,
      manualResolution: {
        executionId: "execution-needs-review",
        method: "settlement",
        venueId: "POLYMARKET",
        contractId: "polymarket:condition:token",
        side: "sell",
        quantity: 5,
        price: 1,
        feeAmountUsd: 0,
        executedAt: "2026-08-17T09:47:07Z",
        externalReference: "claim-0x123",
      },
    })

    render(<OrdersPage />)

    expect(screen.getByText("Manually resolved")).toBeInTheDocument()
    expect(screen.getByText(/Held to settlement \/ claim/)).toBeInTheDocument()
    expect(screen.getByText("+$12.6912 net")).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Resolve manually" })).not.toBeInTheDocument()
  })
})
