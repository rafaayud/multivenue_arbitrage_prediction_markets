import { LoaderCircle, Power, PowerOff, TriangleAlert } from "lucide-react"
import { useState } from "react"

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { TradingRunController } from "@/features/trading/use-trading-run"
import type { SignalSettings } from "@/types/runtime"
import type { TradingRunSettings } from "@/types/trading"

const DEFAULT_SETTINGS: TradingRunSettings = {
  underlyings: [],
  intervals_seconds: [],
  max_arbitrages: 1,
  max_concurrent_arbitrages: 1,
  polymarket_max_notional: 1,
  limitless_max_notional: 1,
  predict_max_notional: 1,
  predict_limit_slippage_ticks: 2,
  predict_use_edge_budget: false,
  min_net_edge: 0,
  cost_buffer: 0,
  max_recovery_loss: 1,
  short_market_keys: [],
}

const DEFAULT_SIGNAL_SETTINGS: SignalSettings = {
  min_net_edge: 0,
  cost_buffer: 0,
}

/** Require risk acknowledgement, credentials and valid limits before starting a live run. */
export function TradingControl({
  controller,
  signalSettings = DEFAULT_SIGNAL_SETTINGS,
  shortMarketKeys = [],
}: {
  controller: TradingRunController
  signalSettings?: SignalSettings
  shortMarketKeys?: string[]
}) {
  const [tradingKey, setTradingKey] = useState("")
  const [riskAcknowledged, setRiskAcknowledged] = useState(false)
  const [maxArbitrages, setMaxArbitrages] = useState(
    String(DEFAULT_SETTINGS.max_arbitrages),
  )
  const [maxConcurrentArbitrages, setMaxConcurrentArbitrages] = useState(
    String(DEFAULT_SETTINGS.max_concurrent_arbitrages),
  )
  const [polymarketMax, setPolymarketMax] = useState(
    String(DEFAULT_SETTINGS.polymarket_max_notional),
  )
  const [limitlessMax, setLimitlessMax] = useState(
    String(DEFAULT_SETTINGS.limitless_max_notional),
  )
  const [predictMax, setPredictMax] = useState(
    String(DEFAULT_SETTINGS.predict_max_notional),
  )
  const [predictSlippageTicks, setPredictSlippageTicks] = useState(
    String(DEFAULT_SETTINGS.predict_limit_slippage_ticks),
  )
  const [predictUseEdgeBudget, setPredictUseEdgeBudget] = useState(false)
  const [maxRecoveryLoss, setMaxRecoveryLoss] = useState(
    String(DEFAULT_SETTINGS.max_recovery_loss),
  )
  const busy = controller.requestState === "loading"
  const enabled =
    controller.run?.status === "preparing" ||
    controller.run?.status === "running" ||
    controller.run?.status === "stopping"
  const error = controller.error ?? controller.run?.error
  const settings = {
    underlyings: DEFAULT_SETTINGS.underlyings,
    intervals_seconds: DEFAULT_SETTINGS.intervals_seconds,
    max_arbitrages: Number(maxArbitrages),
    max_concurrent_arbitrages: Number(maxConcurrentArbitrages),
    polymarket_max_notional: Number(polymarketMax),
    limitless_max_notional: Number(limitlessMax),
    predict_max_notional: Number(predictMax),
    predict_limit_slippage_ticks: Number(predictSlippageTicks),
    predict_use_edge_budget: predictUseEdgeBudget,
    min_net_edge: signalSettings.min_net_edge,
    cost_buffer: signalSettings.cost_buffer,
    max_recovery_loss: Number(maxRecoveryLoss),
    short_market_keys: shortMarketKeys,
  }
  const settingsValid =
    Number.isInteger(settings.max_arbitrages) &&
    settings.max_arbitrages >= 1 &&
    settings.max_arbitrages <= 100 &&
    Number.isInteger(settings.max_concurrent_arbitrages) &&
    settings.max_concurrent_arbitrages >= 1 &&
    settings.max_concurrent_arbitrages <= 100 &&
    settings.polymarket_max_notional >= 1 &&
    settings.polymarket_max_notional <= 10 &&
    settings.limitless_max_notional >= 1 &&
    settings.limitless_max_notional <= 10 &&
    settings.predict_max_notional >= 1 &&
    settings.predict_max_notional <= 10 &&
    Number.isInteger(settings.predict_limit_slippage_ticks) &&
    settings.predict_limit_slippage_ticks >= 0 &&
    settings.max_recovery_loss >= 0 &&
    settings.max_recovery_loss <= 8

  const enable = (allowDegradedVenues: boolean) => {
    const key = tradingKey.trim()
    if (busy || !key || !settingsValid || !riskAcknowledged) return
    setTradingKey("")
    setRiskAcknowledged(false)
    void controller.enable(
      key,
      allowDegradedVenues
        ? { ...settings, allow_degraded_venues: true }
        : settings,
    )
  }

  return (
    <div className="flex flex-col items-end gap-2">
      <div className="flex items-center gap-2">
        <Badge variant={enabled ? "positive" : "secondary"}>
          <span
            className={`size-1.5 rounded-full ${
              enabled ? "bg-emerald-300" : "bg-muted-foreground"
            }`}
          />
          {controller.run?.status === "preparing"
            ? "Preparing short collateral"
            : `Trading ${enabled ? "enabled" : "disabled"}`}
        </Badge>

        {enabled ? (
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="secondary" disabled={busy}>
                <PowerOff className="size-4" />
                Disable trading
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Disable live trading?</AlertDialogTitle>
                <AlertDialogDescription>
                  The worker will stop gracefully. Existing recovery work is
                  allowed to finish before the run stops.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Keep enabled</AlertDialogCancel>
                <AlertDialogAction onClick={() => void controller.disable()}>
                  Disable trading
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        ) : (
          <AlertDialog
            onOpenChange={() => {
              setTradingKey("")
              setRiskAcknowledged(false)
            }}
          >
            <AlertDialogTrigger asChild>
              <Button disabled={busy}>
                {busy ? (
                  <LoaderCircle className="size-4 animate-spin" />
                ) : (
                  <Power className="size-4" />
                )}
                Enable trading
              </Button>
            </AlertDialogTrigger>
            <AlertDialogContent className="max-h-[calc(100vh-2rem)] max-w-4xl overflow-y-auto">
              <AlertDialogHeader>
                <AlertDialogTitle>Enable live trading?</AlertDialogTitle>
                <AlertDialogDescription>
                  This starts the execution worker and can place real orders
                  with real funds. Each venue amount is a maximum per arbitrage;
                  both legs always use the same share quantity. This event
                  workspace never submits recurring crypto markets.
                </AlertDialogDescription>
              </AlertDialogHeader>
              <section
                aria-labelledby="real-money-warning"
                className="mt-4 rounded-xl border border-amber-500/35 bg-amber-500/10 p-4"
              >
                <h3
                  id="real-money-warning"
                  className="flex items-center gap-2 text-sm font-semibold text-amber-800 dark:text-amber-200"
                >
                  <TriangleAlert
                    aria-hidden="true"
                    className="size-5 shrink-0"
                  />
                  Real funds at risk — this is not a simulation
                </h3>
                <ul className="mt-3 list-disc space-y-2 pl-5 text-xs leading-relaxed">
                  <li>
                    A displayed arbitrage signal is not guaranteed profit. One
                    leg can fill while the other fails, leaving an exposed
                    position.
                  </li>
                  <li>
                    Fees, slippage and recovery trades can cause losses. The
                    recovery limit is not a guaranteed cap on your total loss.
                  </li>
                  <li>
                    Funds can remain committed until settlement. Similar event
                    titles may have different resolution rules across venues.
                  </li>
                  <li>
                    Review venue balances, collateral approvals and these limits
                    before starting. Stopping the run does not automatically
                    close existing positions.
                  </li>
                </ul>
                <p className="mt-3 text-xs font-medium">
                  You can inspect signals and feed latency with trading
                  disabled.
                </p>
              </section>
              <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
                <label className="block text-xs font-medium">
                  Arbitrages before stop
                  <input
                    type="number"
                    min="1"
                    max="100"
                    step="1"
                    value={maxArbitrages}
                    onChange={(event) => setMaxArbitrages(event.target.value)}
                    className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </label>
                <label className="block text-xs font-medium">
                  Concurrent arbitrages
                  <input
                    type="number"
                    min="1"
                    max="100"
                    step="1"
                    value={maxConcurrentArbitrages}
                    onChange={(event) =>
                      setMaxConcurrentArbitrages(event.target.value)
                    }
                    className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </label>
                <label className="block text-xs font-medium">
                  Polymarket max (USDC)
                  <input
                    type="number"
                    min="1"
                    max="10"
                    step="0.01"
                    value={polymarketMax}
                    onChange={(event) => setPolymarketMax(event.target.value)}
                    className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </label>
                <label className="block text-xs font-medium">
                  Limitless max (USDC)
                  <input
                    type="number"
                    min="1"
                    max="10"
                    step="0.01"
                    value={limitlessMax}
                    onChange={(event) => setLimitlessMax(event.target.value)}
                    className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </label>
                <label className="block text-xs font-medium">
                  Predict max (USDT)
                  <input
                    type="number"
                    min="1"
                    max="10"
                    step="0.01"
                    value={predictMax}
                    onChange={(event) => setPredictMax(event.target.value)}
                    className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </label>
                <label className="block text-xs font-medium">
                  Predict slippage (ticks)
                  <input
                    type="number"
                    min="0"
                    step="1"
                    value={predictSlippageTicks}
                    disabled={predictUseEdgeBudget}
                    onChange={(event) =>
                      setPredictSlippageTicks(event.target.value)
                    }
                    className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </label>
                <label className="block text-xs font-medium">
                  Recovery max loss (USD)
                  <input
                    type="number"
                    min="0"
                    max="8"
                    step="0.01"
                    value={maxRecoveryLoss}
                    onChange={(event) => setMaxRecoveryLoss(event.target.value)}
                    className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  />
                </label>
              </div>
              <label className="mt-3 flex items-center gap-2 text-xs font-medium">
                <input
                  type="checkbox"
                  checked={predictUseEdgeBudget}
                  onChange={(event) =>
                    setPredictUseEdgeBudget(event.target.checked)
                  }
                />
                Predict: use available edge (experimental)
              </label>
              {predictUseEdgeBudget && (
                <p className="mt-2 text-[11px] text-muted-foreground">
                  Replaces fixed ticks. Keeps the original quantity and the
                  other venue limit, reserving your minimum edge and cost buffer
                  after fees. A one-leg fill can still cause a loss.
                </p>
              )}
              <p className="mt-2 text-[11px] text-muted-foreground">
                Minimum accepted budget: 1 unit of each venue's quote currency
                (USDC or USDT). The actual spend can be lower than the selected
                maximum. Fees are always deducted; edge and buffer are
                additional margins. Every connected event can execute long
                arbitrage; only events marked on the dashboard can prepare and
                execute covered shorts.
              </p>
              {shortMarketKeys.length > 0 && (
                <p className="mt-3 rounded-md border border-amber-400/25 bg-amber-400/10 p-3 text-xs text-amber-200">
                  {shortMarketKeys.length} short market
                  {shortMarketKeys.length === 1 ? " is" : "s are"} selected.
                  Enabling trading can split and lock real venue collateral
                  before SELL orders are allowed.
                </p>
              )}
              <label className="mt-4 block text-xs font-medium">
                Trading API key
                <input
                  type="password"
                  autoComplete="off"
                  value={tradingKey}
                  onChange={(event) => setTradingKey(event.target.value)}
                  className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                />
              </label>
              <label className="mt-4 flex cursor-pointer items-start gap-3 rounded-lg border border-border p-3 text-sm font-medium">
                <input
                  type="checkbox"
                  checked={riskAcknowledged}
                  onChange={(event) =>
                    setRiskAcknowledged(event.target.checked)
                  }
                  className="mt-0.5 size-4 shrink-0 accent-primary"
                />
                I understand this uses real funds and I can lose money.
              </label>
              <p className="mt-3 text-xs leading-relaxed text-muted-foreground">
                “Enable despite degraded” overrides degraded venue-health
                checks; order submission or recovery may fail. Unavailable
                venues still block startup.
              </p>
              <AlertDialogFooter className="flex-wrap">
                <AlertDialogCancel onClick={() => setTradingKey("")}>
                  Cancel
                </AlertDialogCancel>
                <AlertDialogAction
                  className="bg-amber-500 text-black hover:bg-amber-400"
                  disabled={
                    busy ||
                    !tradingKey.trim() ||
                    !settingsValid ||
                    !riskAcknowledged
                  }
                  onClick={() => enable(true)}
                >
                  Enable despite degraded
                </AlertDialogAction>
                <AlertDialogAction
                  disabled={
                    busy ||
                    !tradingKey.trim() ||
                    !settingsValid ||
                    !riskAcknowledged
                  }
                  onClick={() => enable(false)}
                >
                  Enable live trading
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>
        )}
      </div>

      {error && <p className="max-w-sm text-xs text-rose-300">{error}</p>}
    </div>
  )
}
