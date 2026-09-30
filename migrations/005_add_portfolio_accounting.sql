BEGIN;

-- ---------------------------------------------------------------------------
-- Trade replay, fee settlement, and position accounting state.
-- ---------------------------------------------------------------------------

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS fee_settlement_amount NUMERIC;

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS fee_settlement_currency TEXT;

UPDATE trades
SET fee_settlement_amount = fee_amount,
    fee_settlement_currency = 'USD'
WHERE fee_amount IS NOT NULL
  AND fee_settlement_amount IS NULL
  AND fee_currency IN ('USD', 'USDC', 'OUTCOME_TOKEN');

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS journal_sequence BIGINT;

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS venue_id TEXT;

ALTER TABLE trades
    DROP CONSTRAINT IF EXISTS trades_venue_id_check;

ALTER TABLE trades
    ADD CONSTRAINT trades_venue_id_check
    CHECK (venue_id IS NULL OR btrim(venue_id) <> '');

ALTER TABLE trades
    DROP CONSTRAINT IF EXISTS trades_journal_sequence_check;

ALTER TABLE trades
    ADD CONSTRAINT trades_journal_sequence_check
    CHECK (journal_sequence IS NULL OR journal_sequence > 0);

CREATE INDEX IF NOT EXISTS trades_replay_order_idx
    ON trades (journal_sequence, executed_at, trade_id);

CREATE INDEX IF NOT EXISTS trades_venue_contract_replay_idx
    ON trades (venue_id, contract_id, journal_sequence, executed_at, trade_id);

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS venue_id TEXT;

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS current_price NUMERIC;

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS realized_pnl NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS fee_settlement_amount NUMERIC;

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS fee_settlement_currency TEXT;

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE positions
    DROP CONSTRAINT IF EXISTS positions_venue_id_check;

ALTER TABLE positions
    ADD CONSTRAINT positions_venue_id_check
    CHECK (venue_id IS NULL OR btrim(venue_id) <> '');

CREATE INDEX IF NOT EXISTS positions_venue_portfolio_contract_idx
    ON positions (venue_id, portfolio_id, contract_id);

-- ---------------------------------------------------------------------------
-- Principal cash flows between venue portfolios.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cash_movements (
    movement_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('deposit', 'withdrawal', 'transfer')),
    amount NUMERIC NOT NULL CHECK (amount > 0),
    currency TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    source_venue_id TEXT,
    source_portfolio_id TEXT,
    destination_venue_id TEXT,
    destination_portfolio_id TEXT,
    fee_amount NUMERIC CHECK (fee_amount >= 0),
    fee_currency TEXT,
    external_reference TEXT,
    CHECK ((source_venue_id IS NULL) = (source_portfolio_id IS NULL)),
    CHECK ((destination_venue_id IS NULL) = (destination_portfolio_id IS NULL)),
    CHECK (
        (kind = 'deposit' AND source_venue_id IS NULL AND destination_venue_id IS NOT NULL)
        OR (kind = 'withdrawal' AND source_venue_id IS NOT NULL AND destination_venue_id IS NULL)
        OR (
            kind = 'transfer'
            AND source_venue_id IS NOT NULL
            AND destination_venue_id IS NOT NULL
            AND (source_venue_id, source_portfolio_id)
                <> (destination_venue_id, destination_portfolio_id)
        )
    )
);

CREATE INDEX IF NOT EXISTS cash_movements_source_idx
    ON cash_movements (source_venue_id, source_portfolio_id, occurred_at);

CREATE INDEX IF NOT EXISTS cash_movements_destination_idx
    ON cash_movements (
        destination_venue_id,
        destination_portfolio_id,
        occurred_at
    );

-- ---------------------------------------------------------------------------
-- Immutable corrections and historical PnL observations.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS accounting_corrections (
    correction_id TEXT PRIMARY KEY,
    target_trade_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    original_trade JSONB NOT NULL,
    replacement_trade JSONB NOT NULL,
    resulting_position_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS accounting_corrections_trade_idx
    ON accounting_corrections (target_trade_id, recorded_at);

CREATE TABLE IF NOT EXISTS pnl_performance_points (
    point_id TEXT PRIMARY KEY,
    source TEXT NOT NULL CHECK (source IN ('bot_ledger', 'venue_account')),
    venue_id TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    realized_pnl_usd NUMERIC,
    unrealized_pnl_usd NUMERIC,
    fees_usd NUMERIC,
    total_pnl_usd NUMERIC,
    scope TEXT NOT NULL,
    partial BOOLEAN NOT NULL DEFAULT FALSE,
    quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS pnl_performance_points_range_idx
    ON pnl_performance_points (source, observed_at);

COMMIT;
