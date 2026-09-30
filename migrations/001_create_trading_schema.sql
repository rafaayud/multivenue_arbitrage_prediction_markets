BEGIN;

-- ---------------------------------------------------------------------------
-- Trading ledger: executed fills and open inventory.
-- ---------------------------------------------------------------------------

-- Individual venue fills linked to orders and contracts.
CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    order_id TEXT,
    client_order_id TEXT,
    contract_id TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC NOT NULL CHECK (quantity > 0),
    -- Outcome prices are probability-style quotes in [0, 1].
    price NUMERIC NOT NULL CHECK (price >= 0 AND price <= 1),
    executed_at TIMESTAMPTZ NOT NULL,
    -- Durable ordering key from the append-only application journal.
    journal_sequence BIGINT CHECK (journal_sequence > 0),
    portfolio_id TEXT,
    strategy_id TEXT,
    -- Taker/maker fee charged by the venue for this fill, when known.
    fee_amount NUMERIC,
    fee_currency TEXT
);

CREATE INDEX IF NOT EXISTS trades_order_id_idx ON trades (order_id);
CREATE INDEX IF NOT EXISTS trades_contract_id_idx ON trades (contract_id);

-- Compatibility for databases created before fee columns existed.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS fee_amount NUMERIC;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS fee_currency TEXT;

-- Net position per contract after fills and flat markers.
CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    contract_id TEXT NOT NULL,
    venue_id TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC NOT NULL CHECK (quantity >= 0),
    average_entry_price NUMERIC,
    current_price NUMERIC,
    realized_pnl NUMERIC NOT NULL DEFAULT 0,
    fee_settlement_amount NUMERIC,
    fee_settlement_currency TEXT,
    quality_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    portfolio_id TEXT,
    opened_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS positions_contract_id_idx ON positions (contract_id);
-- Speeds up “what is still open?” queries; flat rows are excluded.
CREATE INDEX IF NOT EXISTS positions_open_idx ON positions (side) WHERE side != 'flat';

-- ---------------------------------------------------------------------------
-- Order lifecycle: intent submitted to venues and local fill progress.
-- ---------------------------------------------------------------------------

-- Local order snapshot keyed by a stable order_key.
-- Intentionally omits filled_quantity <= quantity: a buy can receive more
-- contracts than requested when it matches below its limit price.
CREATE TABLE IF NOT EXISTS orders (
    order_key TEXT PRIMARY KEY,
    client_order_id TEXT UNIQUE,
    order_id TEXT UNIQUE,
    contract_id TEXT NOT NULL,
    status TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC NOT NULL CHECK (quantity > 0),
    order_type TEXT NOT NULL,
    limit_price NUMERIC CHECK (limit_price >= 0 AND limit_price <= 1),
    filled_quantity NUMERIC NOT NULL DEFAULT 0 CHECK (filled_quantity >= 0),
    average_price NUMERIC CHECK (average_price >= 0 AND average_price <= 1),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    -- At least one venue/local identifier must be present.
    CHECK (client_order_id IS NOT NULL OR order_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS orders_contract_id_idx ON orders (contract_id);
-- Active working orders for dashboards and settlers.
CREATE INDEX IF NOT EXISTS orders_open_idx ON orders (status)
    WHERE status IN ('submitted', 'accepted', 'partially_filled');

-- Compatibility for databases created with the old filled_quantity <= quantity check.
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_check1;

-- ---------------------------------------------------------------------------
-- Execution ops: residual exposure recovery and two-leg arbitrage journals.
-- ---------------------------------------------------------------------------

-- Tracks leftover inventory after a partial/failed hedge until resolved.
CREATE TABLE IF NOT EXISTS exposure_recoveries (
    recovery_id TEXT PRIMARY KEY,
    venue_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity NUMERIC NOT NULL,
    limit_price NUMERIC NOT NULL CHECK (limit_price >= 0 AND limit_price <= 1),
    portfolio_id TEXT,
    strategy_id TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'attempting', 'resolved', 'needs_review')
    ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    client_order_id TEXT,
    order_id TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    -- Zero quantity is only allowed when the recovery needs manual review.
    CHECK (quantity > 0 OR status = 'needs_review')
);

CREATE INDEX IF NOT EXISTS exposure_recoveries_unresolved_idx
    ON exposure_recoveries (status, updated_at)
    WHERE status != 'resolved';

-- Write-side journal for a planned two-leg arbitrage execution.
CREATE TABLE IF NOT EXISTS arbitrage_execution_journals (
    execution_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (
        status IN (
            'planned', 'hedge_pending', 'unwind_pending',
            'accounting_pending', 'completed', 'needs_review'
        )
    ),
    -- Primary leg.
    leg1_venue_id TEXT NOT NULL,
    leg1_contract_id TEXT NOT NULL,
    leg1_side TEXT NOT NULL CHECK (leg1_side IN ('buy', 'sell')),
    leg1_quantity NUMERIC NOT NULL CHECK (leg1_quantity > 0),
    leg1_limit_price NUMERIC NOT NULL CHECK (
        leg1_limit_price >= 0 AND leg1_limit_price <= 1
    ),
    leg1_client_order_id TEXT NOT NULL,
    leg1_order_id TEXT,
    leg1_filled_quantity NUMERIC NOT NULL DEFAULT 0 CHECK (
        leg1_filled_quantity >= 0
    ),
    -- Hedge / secondary leg.
    leg2_venue_id TEXT NOT NULL,
    leg2_contract_id TEXT NOT NULL,
    leg2_side TEXT NOT NULL CHECK (leg2_side IN ('buy', 'sell')),
    leg2_quantity NUMERIC NOT NULL CHECK (leg2_quantity > 0),
    leg2_limit_price NUMERIC NOT NULL CHECK (
        leg2_limit_price >= 0 AND leg2_limit_price <= 1
    ),
    leg2_client_order_id TEXT NOT NULL,
    leg2_order_id TEXT,
    leg2_filled_quantity NUMERIC NOT NULL DEFAULT 0 CHECK (
        leg2_filled_quantity >= 0
    ),
    -- Unhedged quantity left after fills; drives recovery when > 0.
    residual_quantity NUMERIC NOT NULL DEFAULT 0 CHECK (residual_quantity >= 0),
    portfolio_id TEXT,
    strategy_id TEXT,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS arbitrage_execution_journals_active_idx
    ON arbitrage_execution_journals (status, updated_at)
    WHERE status != 'completed';

COMMIT;
