"""Define the JSON contracts exposed by the HTTP API.

Responsibilities
----------------
- Represent request and response payloads with validated Pydantic models.
"""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class TradingSessionStart(BaseModel):
    trading_key: str = Field(min_length=1)


class TradingRunStart(BaseModel):
    """Validate the explicit confirmation and risk limits required to start live trading."""

    live: Literal[True]
    confirmation: Literal["LIVE"]
    allow_degraded_venues: bool = Field(
        default=False,
        description="Allow this run to start when venues are degraded but available.",
    )
    underlyings: tuple[
        Literal[
            "BTC",
            "ETH",
            "BNB",
            "NVDA",
            "AMZN",
            "META",
            "TSLA",
            "SPY",
            "SPCX",
        ],
        ...,
    ] = (
        "BTC",
        "ETH",
        "BNB",
        "NVDA",
        "AMZN",
        "META",
        "TSLA",
        "SPY",
        "SPCX",
    )
    intervals_seconds: tuple[Literal[3600, 86400], ...] = (3600, 86400)
    max_arbitrages: int = Field(
        default=1,
        ge=1,
        le=100,
        description="Stop after this many completed or recovered executions.",
    )
    max_concurrent_arbitrages: int = Field(default=2, ge=1, le=100)
    min_net_edge: Decimal = Field(default=Decimal("0"), ge=0, le=Decimal("0.20"))
    cost_buffer: Decimal = Field(
        default=Decimal("0"),
        ge=0,
        le=Decimal("0.05"),
        description="Additional cost allowance deducted during detection.",
    )
    max_recovery_loss: Decimal = Field(
        default=Decimal("1"),
        ge=0,
        le=Decimal("8"),
        description="Maximum estimated USD loss accepted for one recovery order.",
    )
    polymarket_max_notional: Decimal = Field(
        default=Decimal("5"),
        ge=Decimal("1"),
        le=Decimal("10"),
    )
    limitless_max_notional: Decimal = Field(
        default=Decimal("5"),
        ge=Decimal("1"),
        le=Decimal("10"),
    )
    predict_max_notional: Decimal = Field(
        default=Decimal("5"),
        ge=Decimal("1"),
        le=Decimal("10"),
    )
    predict_limit_slippage_ticks: int = Field(
        default=2,
        ge=0,
        description=(
            "Predict-only execution-limit headroom in ticks; zero disables it."
        ),
    )
    predict_use_edge_budget: bool = Field(
        default=False,
        description=(
            "Replace fixed Predict ticks with fee-aware edge-budget pricing; "
            "preserve quantity, venue budgets, minimum net edge and cost buffer."
        ),
    )
    short_market_keys: tuple[
        Annotated[str, Field(min_length=1, max_length=512)],
        ...,
    ] = Field(default=(), max_length=100)


class SignalSettings(BaseModel):
    """Validate fee-aware thresholds applied during opportunity detection.

    Attributes
    ----------
    min_net_edge
        Minimum profit required per contract after fees and cost buffer.
    cost_buffer
        Additional cost allowance deducted per contract before emission.
    """

    min_net_edge: Decimal = Field(default=Decimal("0"), ge=0, le=Decimal("0.20"))
    cost_buffer: Decimal = Field(default=Decimal("0"), ge=0, le=Decimal("0.05"))


class VenueHealthOut(BaseModel):
    """Represent one venue's current trading availability."""

    venue_id: str
    status: Literal["operational", "degraded", "unavailable"]
    checked_at: datetime
    latency_ms: float | None
    source: str
    message: str
    error_type: str | None
    http_status: int | None
    retryable: bool


class VenueHealthReportOut(BaseModel):
    """Return one cached cross-venue health report."""

    generated_at: datetime
    overall_status: Literal["operational", "degraded", "unavailable"]
    venues: list[VenueHealthOut]


class ContractOut(BaseModel):
    """Represent a normalized binary contract in an API response."""
    id: str
    market_id: str
    outcome_id: str
    venue_id: str
    symbol: str | None = None


class MatchPairOut(BaseModel):
    """Represent one complementary cross-venue contract pair."""
    left: ContractOut
    right: ContractOut


class MarketMatchesResponse(BaseModel):
    """Return contract matches for one underlying and interval."""
    underlying: str
    interval_seconds: int
    pairs: list[MatchPairOut]


class MonitoredMarketMatchesOut(BaseModel):
    """Return matches and identity metadata for one monitored market cycle."""

    monitor_key: str
    family: str
    underlying: str
    interval_seconds: int
    pairs: list[MatchPairOut]


class ArbitrageVenueMarketOut(BaseModel):
    """Represent one venue market inside an arbitrage candidate."""

    venue_id: str
    market_id: str
    external_market_id: str
    yes_outcome_id: str
    no_outcome_id: str
    title: str | None
    volume_usd: Decimal | None


class ArbitrageCandidateOut(BaseModel):
    """Represent one normalized arbitrage candidate."""

    market_id: str
    title: str
    event_title: str | None
    venue_event_id: str | None
    return_rate: Decimal
    observed_at: datetime
    starts_at: datetime | None
    ends_at: datetime | None
    volume_usd: Decimal | None
    liquidity_usd: Decimal | None
    liquidity_tier: Literal["deep", "shallow"] | None
    markets: list[ArbitrageVenueMarketOut]


class RegularMarketSelectionIn(BaseModel):
    """Select one native venue market for regular monitoring."""

    venue_id: str = Field(min_length=1)
    external_market_id: str = Field(min_length=1)
    search_text: str | None = Field(default=None, min_length=1)


class RegularMarketMonitorIn(BaseModel):
    """Select the cross-venue markets that form one regular candidate."""

    markets: tuple[RegularMarketSelectionIn, ...] = Field(min_length=2)


class RegularMarketMonitorOut(BaseModel):
    """Report the regular candidate added to runtime monitoring."""

    monitor_key: str
    pair_count: int
    venue_ids: tuple[str, ...]


class SignalData(BaseModel):
    """Represent one executable arbitrage signal in JSON form."""
    contract_id: str
    venue_id: str
    direction: str
    quantity: Decimal
    limit_price: Decimal
    fair_probability: Decimal
    edge: Decimal
    strategy_id: str
    generated_at: datetime


class SignalResponse(BaseModel):
    """Return one paired arbitrage signal update."""
    type: Literal["arbitrage_signal_pair"]
    monitor_type: Literal["cycle", "regular"]
    monitor_key: str
    market_label: str
    underlying: str | None = None
    interval_seconds: int | None = None
    signals: list[SignalData]


class ArbitrageOpportunityOut(BaseModel):
    """Represent one journal-projected arbitrage opportunity."""

    id: str
    journal_sequence: int
    monitor_type: Literal["cycle", "regular"]
    monitor_key: str
    underlying: str | None
    interval_seconds: int | None
    side: str
    left_contract_id: str
    right_contract_id: str
    left_price: Decimal
    right_price: Decimal
    quantity: Decimal
    gross_edge: Decimal
    net_edge: Decimal
    fee_per_contract: Decimal
    total_fees: Decimal
    skew_ns: int
    detected_at: datetime


class TradingPreflightOut(BaseModel):
    """Report whether live trading dependencies are ready."""

    ready: bool
    missing_credentials: list[str]
    database_ready: bool
    active_journals: int | None
    unresolved_recoveries: int | None
    venue_health_status: Literal["operational", "degraded", "unavailable"]
    venue_health_issues: list[str] = Field(default_factory=list)


class TradingRunOut(BaseModel):
    """Represent the externally visible state of a live trading run."""
    id: str
    status: Literal[
        "preparing",
        "running",
        "stopping",
        "stopped",
        "completed",
        "failed",
    ]
    started_at: datetime
    finished_at: datetime | None
    error: str | None
    short_market_keys: tuple[str, ...]


class OrderOut(BaseModel):
    status: str
    contract_id: str
    side: str
    quantity: Decimal
    order_type: str
    client_order_id: str | None
    order_id: str | None
    limit_price: Decimal | None
    filled_quantity: Decimal
    average_price: Decimal | None
    created_at: datetime | None
    updated_at: datetime | None


class TradeOut(BaseModel):
    trade_id: str
    order_id: str | None
    client_order_id: str | None
    contract_id: str
    side: str
    quantity: Decimal
    price: Decimal
    executed_at: datetime
    portfolio_id: str | None
    strategy_id: str | None
    fee_amount: Decimal | None
    fee_currency: str | None
    fee_settlement_amount: Decimal | None = None
    fee_settlement_currency: str | None = None
    venue_id: str = "legacy"


class AccountingCorrectionIn(BaseModel):
    """Replace the economic fields of one existing trade."""

    trade_id: str = Field(min_length=1)
    side: Literal["buy", "sell"]
    quantity: Decimal = Field(gt=0)
    price: Decimal = Field(ge=0, le=1)
    executed_at: datetime
    reason: str = Field(min_length=1, max_length=500)
    fee_amount: Decimal | None = Field(default=None, ge=0)
    fee_currency: str | None = None
    fee_settlement_amount: Decimal | None = Field(default=None, ge=0)
    fee_settlement_currency: str | None = None


class AccountingCorrectionOut(BaseModel):
    """Confirm the durable identity and affected position of a correction."""

    correction_id: str
    trade_id: str
    position_id: str
    recorded_at: datetime


class PositionOut(BaseModel):
    position_id: str
    contract_id: str
    side: str
    quantity: Decimal
    average_entry_price: Decimal | None
    portfolio_id: str | None
    opened_at: datetime | None
    updated_at: datetime | None
    venue_id: str = "legacy"
    current_price: Decimal | None = None
    realized_pnl: Decimal = Decimal("0")
    fee_settlement_amount: Decimal | None = None
    fee_settlement_currency: str | None = None
    quality_flags: tuple[str, ...] = ()


class ExposureRecoveryOut(BaseModel):
    recovery_id: str
    execution_id: str | None = None
    route: str | None = None
    venue_id: str
    contract_id: str
    side: str
    quantity: Decimal
    filled_quantity: Decimal = Decimal("0")
    limit_price: Decimal
    average_price: Decimal | None = None
    source_contract_id: str | None = None
    source_side: str | None = None
    source_price: Decimal | None = None
    source_fee_amount: Decimal | None = None
    source_fee_currency: str | None = None
    estimated_vwap: Decimal | None = None
    estimated_recovery_fee_amount: Decimal | None = None
    estimated_recovery_fee_currency: str | None = None
    estimated_gross_result: Decimal | None = None
    estimated_net_result: Decimal | None = None
    recovery_fee_amount: Decimal | None = None
    recovery_fee_currency: str | None = None
    actual_gross_result: Decimal | None = None
    actual_net_result: Decimal | None = None
    portfolio_id: str | None
    strategy_id: str | None
    status: str
    attempts: int
    client_order_id: str | None
    order_id: str | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class ExecutionLegOut(BaseModel):
    venue_id: str
    contract_id: str
    side: str
    quantity: Decimal
    limit_price: Decimal
    client_order_id: str
    order_id: str | None
    filled_quantity: Decimal
    average_fill_price: Decimal | None = None
    fee_amount: Decimal | None = None
    fee_currency: str | None = None
    fee_settlement_amount: Decimal | None = None
    fee_settlement_currency: str | None = None


class ManualExecutionResolutionIn(BaseModel):
    """Validate one externally completed residual exposure."""

    method: Literal["manual_sale", "settlement"]
    price: Decimal = Field(ge=0, le=1)
    fee_amount_usd: Decimal = Field(default=Decimal("0"), ge=0)
    executed_at: datetime
    external_reference: str | None = Field(default=None, max_length=256)


class ManualExecutionResolutionOut(BaseModel):
    """Expose the inferred accounting trade recorded by an operator."""

    execution_id: str
    method: Literal["manual_sale", "settlement"]
    venue_id: str
    contract_id: str
    side: str
    quantity: Decimal
    price: Decimal
    fee_amount_usd: Decimal
    executed_at: datetime
    external_reference: str | None = None


class ExecutionJournalOut(BaseModel):
    execution_id: str
    monitor_type: Literal["cycle", "regular"] | None = None
    monitor_key: str | None = None
    underlying: str | None = None
    interval_seconds: int | None = None
    status: str
    leg1: ExecutionLegOut
    leg2: ExecutionLegOut
    residual_quantity: Decimal
    gross_locked_pnl_usd: Decimal | None = None
    total_fee_settlement_cost_usd: Decimal | None = None
    net_locked_pnl_usd: Decimal | None = None
    manual_resolution: ManualExecutionResolutionOut | None = None
    latency_trace: dict[str, object] | None = None
    portfolio_id: str | None
    strategy_id: str | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class PnlPointOut(BaseModel):
    """Represent one consolidated venue PnL observation."""

    observed_at: datetime
    net_pnl_usd: Decimal


class InternalVenuePnlOut(BaseModel):
    """Represent one venue's contribution to internally calculated PnL."""

    venue_id: str
    gross_contribution_usd: Decimal
    fees_usd: Decimal
    net_contribution_usd: Decimal
    execution_count: int


class InternalPnlSummaryOut(BaseModel):
    """Summarize all-time PnL calculated from terminal bot executions."""

    scope: Literal["bot_terminal_executions_all_time"] = (
        "bot_terminal_executions_all_time"
    )
    gross_pnl_usd: Decimal
    fees_usd: Decimal
    net_pnl_usd: Decimal
    priced_terminal_executions: int
    unpriced_terminal_executions: int
    venues: list[InternalVenuePnlOut]
    series: list[PnlPointOut]


class PnlDashboardOut(BaseModel):
    """Return one coherent portfolio view plus secondary diagnostics."""

    generated_at: datetime
    portfolio_performance: "PortfolioPerformanceOut"
    terminal_executions: InternalPnlSummaryOut
    reconciliation: "PnlReconciliationOut"
    venue_health: list["PnlVenueHealthRowOut"]
    positions: list[PositionOut]


class PnlPerformanceSummaryOut(BaseModel):
    realized: Decimal | None
    unrealized: Decimal | None
    total: Decimal | None
    fees: Decimal | None
    gas: Decimal | None
    return_pct: Decimal | None = None


class PnlComparabilityOut(BaseModel):
    comparable: bool
    confidence: Literal["high", "medium", "low"]
    notes: list[str]


class PerformanceViewOut(BaseModel):
    scope_label: str
    methodology: str
    requested_range: Literal["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"]
    effective_range: Literal["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"]
    range_supported: bool
    methodology_note: str | None
    summary: PnlPerformanceSummaryOut
    series: list[PnlPointOut]
    comparability: PnlComparabilityOut
    partial: bool
    last_updated: datetime
    quality_flags: list[str] = Field(default_factory=list)


class PortfolioPerformanceOut(BaseModel):
    default_view: Literal["bot_ledger"] = "bot_ledger"
    selected_view: Literal["venue_account", "bot_ledger"]
    selected_range: Literal["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"]
    available_ranges: list[
        Literal["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"]
    ]
    venue_account: PerformanceViewOut
    bot_ledger: PerformanceViewOut


class InternalVenuePnlV2Out(BaseModel):
    venue_id: str
    realized_pnl_usd: Decimal
    unrealized_pnl_usd: Decimal | None
    fees_usd: Decimal | None
    total_pnl_usd: Decimal | None
    partial: bool


class PnlReconciliationOut(BaseModel):
    internal_net_pnl_usd: Decimal | None
    venue_reported_net_pnl_usd: Decimal | None
    difference_usd: Decimal | None
    notes: list[str]
    internal_venues: list[InternalVenuePnlV2Out]


class PnlVenueHealthRowOut(BaseModel):
    venue_id: str
    observed_at: datetime | None
    stale: bool
    scope: str | None
    missing_fees: bool
    status: Literal["operational", "degraded", "unavailable"]
    notes: list[str]


class ExecutionActivityOut(BaseModel):
    type: Literal["execution_activity_snapshot"] = "execution_activity_snapshot"
    generated_at: datetime
    trading_fees_usd: Decimal
    gas_usd: Decimal
    orders: list[OrderOut]
    trades: list[TradeOut]
    positions: list[PositionOut]
    journals: list[ExecutionJournalOut]
    recoveries: list[ExposureRecoveryOut]


class AlertmanagerWebhookAlert(BaseModel):
    """Represent one alert in an Alertmanager webhook payload."""

    status: Literal["firing", "resolved"]
    labels: dict[str, str]
    annotations: dict[str, str] = Field(default_factory=dict)
    starts_at: datetime = Field(alias="startsAt")
    ends_at: datetime | None = Field(default=None, alias="endsAt")
    generator_url: str = Field(default="", alias="generatorURL")
    fingerprint: str = Field(min_length=1, max_length=256)


class AlertmanagerWebhook(BaseModel):
    """Validate the Alertmanager webhook v4 contract received by the API."""

    version: Literal["4"]
    group_key: str = Field(alias="groupKey")
    truncated_alerts: int = Field(alias="truncatedAlerts", ge=0)
    status: Literal["firing", "resolved"]
    receiver: str = Field(min_length=1)
    group_labels: dict[str, str] = Field(alias="groupLabels")
    common_labels: dict[str, str] = Field(alias="commonLabels")
    common_annotations: dict[str, str] = Field(alias="commonAnnotations")
    external_url: str = Field(alias="externalURL")
    alerts: list[AlertmanagerWebhookAlert] = Field(min_length=1, max_length=100)
