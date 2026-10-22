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
import { readTradingPreferences, saveTradingPreferences } from "@/features/trading/trading-preferences"
import { apiClient } from "@/lib/api-client"
import type { SignalSettings } from "@/types/runtime"
import type { InventorySellPair, TradingRunSettings } from "@/types/trading"

const DEFAULT_SETTINGS: TradingRunSettings = {
  max_arbitrages: 1,
  max_concurrent_arbitrages: 1,
  polymarket_max_notional: 1,
  limitless_max_notional: 1,
  predict_max_notional: 1,
  predict_limit_slippage_ticks: 0,
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
  existingInventoryPairs = [],
  manualInventoryMarketKeys = shortMarketKeys,
}: {
  controller: TradingRunController
  signalSettings?: SignalSettings
  shortMarketKeys?: string[]
  existingInventoryPairs?: InventorySellPair[]
  manualInventoryMarketKeys?: string[]
}) {
  const [savedSettings, setSavedSettings] = useState(readTradingPreferences)
  const [tradingKey, setTradingKey] = useState("")
  const [riskAcknowledged, setRiskAcknowledged] = useState(false)
  const [manualSplitsDeclared, setManualSplitsDeclared] = useState(false)
  const [importingSplits, setImportingSplits] = useState(false)
  const [importResult, setImportResult] = useState<string | null>(null)
  const [importError, setImportError] = useState<string | null>(null)
  const [maxArbitrages, setMaxArbitrages] = useState(
    String(savedSettings.max_arbitrages ?? DEFAULT_SETTINGS.max_arbitrages),
  )
  const [maxConcurrentArbitrages, setMaxConcurrentArbitrages] = useState(
    String(savedSettings.max_concurrent_arbitrages ?? DEFAULT_SETTINGS.max_concurrent_arbitrages),
  )
  const [polymarketMax, setPolymarketMax] = useState(
    String(savedSettings.polymarket_max_notional ?? DEFAULT_SETTINGS.polymarket_max_notional),
  )
  const [limitlessMax, setLimitlessMax] = useState(
    String(savedSettings.limitless_max_notional ?? DEFAULT_SETTINGS.limitless_max_notional),
  )
  const [predictMax, setPredictMax] = useState(
    String(savedSettings.predict_max_notional ?? DEFAULT_SETTINGS.predict_max_notional),
  )
  const [minNetEdge, setMinNetEdge] = useState<string | null>(null)
  const [costBuffer, setCostBuffer] = useState<string | null>(null)
  const [maxRecoveryLoss, setMaxRecoveryLoss] = useState(
    String(savedSettings.max_recovery_loss ?? DEFAULT_SETTINGS.max_recovery_loss),
  )
  const busy = controller.requestState === "loading" || importingSplits
  const enabled =
    controller.run?.status === "preparing" ||
    controller.run?.status === "running" ||
    controller.run?.status === "stopping"
  const error = controller.error ?? controller.run?.error
  const minNetEdgeValue = minNetEdge ?? String(signalSettings.min_net_edge)
  const costBufferValue = costBuffer ?? String(signalSettings.cost_buffer)
  const settings = {
    max_arbitrages: Number(maxArbitrages),
    max_concurrent_arbitrages: Number(maxConcurrentArbitrages),
    polymarket_max_notional: Number(polymarketMax),
    limitless_max_notional: Number(limitlessMax),
    predict_max_notional: Number(predictMax),
    predict_limit_slippage_ticks: DEFAULT_SETTINGS.predict_limit_slippage_ticks,
    predict_use_edge_budget: DEFAULT_SETTINGS.predict_use_edge_budget,
    min_net_edge: Number(minNetEdgeValue),
    cost_buffer: Number(costBufferValue),
    max_recovery_loss: Number(maxRecoveryLoss),
    short_market_keys: shortMarketKeys,
    ...(existingInventoryPairs.length > 0
      ? { existing_inventory_pairs: existingInventoryPairs }
      : {}),
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
    minNetEdgeValue.trim() !== "" &&
    settings.min_net_edge >= 0 &&
    settings.min_net_edge <= 0.2 &&
    costBufferValue.trim() !== "" &&
    settings.cost_buffer >= 0 &&
    settings.cost_buffer <= 0.05 &&
    settings.max_recovery_loss >= 0 &&
    settings.max_recovery_loss <= 8

  const enable = (allowDegradedVenues: boolean) => {
    const key = tradingKey.trim()
    if (busy || !key || !settingsValid || !riskAcknowledged) return
    saveTradingPreferences(settings)
    setSavedSettings(settings)
    setTradingKey("")
    setRiskAcknowledged(false)
    void controller.enable(
      key,
      allowDegradedVenues
        ? { ...settings, allow_degraded_venues: true }
        : settings,
    )
  }

  const importManualSplits = async () => {
    if (busy || !tradingKey.trim() || !manualSplitsDeclared) return
    setImportingSplits(true)
    setImportResult(null)
    setImportError(null)
    try {
      const imported = await apiClient.importManualSplits(tradingKey.trim(), manualInventoryMarketKeys)
      setImportResult(imported.length
        ? `Registered ${imported.map((item) => `${item.quantity} YES + ${item.quantity} NO on ${item.venue_id}`).join("; ")}. Saved for future runs.`
        : "No new complete sets to register. Existing inventory is already accounted for.")
    } catch (reason) {
      setImportError(reason instanceof Error ? reason.message : "Manual split import failed")
    } finally {
      setImportingSplits(false)
    }
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
            ? (controller.run?.existing_inventory_pairs?.length
              ? "Checking sell inventory" : "Preparing short collateral")
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
                  The runtime will stop gracefully. Existing recovery work is
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
            onOpenChange={(open) => {
              setTradingKey("")
              setRiskAcknowledged(false)
              setManualSplitsDeclared(false)
              setImportResult(null)
              setImportError(null)
              if (open) {
                setMinNetEdge(savedSettings.min_net_edge == null ? null : String(savedSettings.min_net_edge))
                setCostBuffer(savedSettings.cost_buffer == null ? null : String(savedSettings.cost_buffer))
              }
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
                  This starts trading and can place real orders
                  with real funds. Each venue amount is a maximum per arbitrage;
                  both legs always use the same share quantity. This event
                  workspace trades only explicitly selected events.
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
              <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
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
              <fieldset className="mt-4 rounded-xl border border-border p-4">
                <legend className="px-1 text-sm font-medium">Profit requirements</legend>
                <div className="grid gap-3 sm:grid-cols-2">
                  <label className="block text-xs font-medium">
                    Minimum net edge (USD/contract)
                    <input
                      type="number"
                      min="0"
                      max="0.2"
                      step="0.001"
                      value={minNetEdgeValue}
                      onChange={(event) => setMinNetEdge(event.target.value)}
                      className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    />
                  </label>
                  <label className="block text-xs font-medium">
                    Cost buffer (USD/contract)
                    <input
                      type="number"
                      min="0"
                      max="0.05"
                      step="0.001"
                      value={costBufferValue}
                      onChange={(event) => setCostBuffer(event.target.value)}
                      className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    />
                  </label>
                </div>
                <p className="mt-3 text-xs leading-relaxed text-muted-foreground">
                  Fees and the cost buffer are deducted before checking the minimum
                  net edge. These values apply when you confirm this trading run.
                </p>
              </fieldset>
              <p className="mt-2 text-[11px] text-muted-foreground">
                Minimum accepted budget: 1 unit of each venue's quote currency
                (USDC or USDT). The actual spend can be lower than the selected
                maximum. Every connected event can execute long
                arbitrage. SELL orders use the inventory routes selected on the
                dashboard; collateral preparation requires its separate event toggle.
              </p>
              {shortMarketKeys.length > 0 && (
                <p className="mt-3 rounded-md border border-amber-400/25 bg-amber-400/10 p-3 text-xs text-amber-200">
                  {shortMarketKeys.length} short market
                  {shortMarketKeys.length === 1 ? " is" : "s are"} selected.
                  Enabling trading can split and lock real venue collateral
                  before SELL orders are allowed.
                </p>
              )}
              {existingInventoryPairs.length > 0 && (
                <p className="mt-3 rounded-md border border-border bg-muted/30 p-3 text-xs">
                  Sell existing inventory: {existingInventoryPairs.length} selected pair
                  {existingInventoryPairs.length === 1 ? "" : "s"}.
                  Wallet balances and accounted tokens are checked before trading.
                  These pairs never split collateral or replenish tokens automatically;
                  sales stop when either selected token runs out.
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
              {manualInventoryMarketKeys.length > 0 && (
                <section className="mt-4 space-y-3 rounded-xl border border-border bg-muted/30 p-4">
                  <h3 className="text-sm font-semibold">Already split manually?</h3>
                  <p className="text-xs leading-relaxed text-muted-foreground">
                    Register unaccounted YES/NO pairs in events selected for
                    collateral preparation or existing-inventory sales.
                    Their cost is 1 collateral unit per pair, allocated equally to
                    YES and NO. This is saved across restarts; repeating it will
                    not count the same tokens twice.
                  </p>
                  <label className="flex items-start gap-3 text-xs">
                    <input
                      type="checkbox"
                      checked={manualSplitsDeclared}
                      onChange={(event) => setManualSplitsDeclared(event.target.checked)}
                      className="mt-0.5 size-4 shrink-0 accent-primary"
                    />
                    I created these extra pairs by splitting collateral manually,
                    with no gas or fees charged to me.
                  </label>
                  <Button
                    type="button"
                    variant="secondary"
                    disabled={busy || !tradingKey.trim() || !manualSplitsDeclared}
                    onClick={() => void importManualSplits()}
                  >
                    {importingSplits && <LoaderCircle className="size-4 animate-spin" />}
                    Register manual splits
                  </Button>
                  <p className="text-xs text-muted-foreground">
                    Reads balances and records inventory only. Trading stays disabled.
                  </p>
                  {importResult && <p role="status" className="text-xs text-emerald-600 dark:text-emerald-300">{importResult}</p>}
                  {importError && <p role="alert" className="text-xs text-rose-600 dark:text-rose-300">{importError}</p>}
                </section>
              )}
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
