BEGIN;

-- ---------------------------------------------------------------------------
-- Execution lifecycle, latency diagnostics, and automatic recovery.
-- ---------------------------------------------------------------------------

ALTER TABLE arbitrage_execution_journals
    DROP CONSTRAINT IF EXISTS arbitrage_execution_journals_status_check;

ALTER TABLE arbitrage_execution_journals
    ADD CONSTRAINT arbitrage_execution_journals_status_check CHECK (
        status IN (
            'planned', 'primary_pending', 'hedge_pending', 'recovery_pending',
            'unwind_pending', 'accounting_pending', 'completed', 'recovered',
            'needs_review', 'rejected'
        )
    );

ALTER TABLE arbitrage_execution_journals
    ADD COLUMN IF NOT EXISTS latency_trace JSONB;

ALTER TABLE execution_order_commands
    DROP CONSTRAINT IF EXISTS execution_order_commands_role_check;

ALTER TABLE execution_order_commands
    ADD CONSTRAINT execution_order_commands_role_check CHECK (
        role IN ('primary', 'hedge', 'recovery')
    );

ALTER TABLE exposure_recoveries
    ADD COLUMN IF NOT EXISTS execution_id TEXT,
    ADD COLUMN IF NOT EXISTS route TEXT CHECK (
        route IN ('complete_missing_leg', 'unwind_excess')
    ),
    ADD COLUMN IF NOT EXISTS source_contract_id TEXT,
    ADD COLUMN IF NOT EXISTS source_side TEXT CHECK (source_side IN ('buy', 'sell')),
    ADD COLUMN IF NOT EXISTS source_price NUMERIC CHECK (
        source_price >= 0 AND source_price <= 1
    ),
    ADD COLUMN IF NOT EXISTS source_fee_amount NUMERIC,
    ADD COLUMN IF NOT EXISTS source_fee_currency TEXT,
    ADD COLUMN IF NOT EXISTS estimated_vwap NUMERIC CHECK (
        estimated_vwap >= 0 AND estimated_vwap <= 1
    ),
    ADD COLUMN IF NOT EXISTS estimated_recovery_fee_amount NUMERIC,
    ADD COLUMN IF NOT EXISTS estimated_recovery_fee_currency TEXT,
    ADD COLUMN IF NOT EXISTS estimated_gross_result NUMERIC,
    ADD COLUMN IF NOT EXISTS estimated_net_result NUMERIC,
    ADD COLUMN IF NOT EXISTS filled_quantity NUMERIC NOT NULL DEFAULT 0 CHECK (
        filled_quantity >= 0
    ),
    ADD COLUMN IF NOT EXISTS average_price NUMERIC CHECK (
        average_price >= 0 AND average_price <= 1
    ),
    ADD COLUMN IF NOT EXISTS recovery_fee_amount NUMERIC,
    ADD COLUMN IF NOT EXISTS recovery_fee_currency TEXT,
    ADD COLUMN IF NOT EXISTS actual_gross_result NUMERIC,
    ADD COLUMN IF NOT EXISTS actual_net_result NUMERIC;

CREATE INDEX IF NOT EXISTS exposure_recoveries_execution_idx
    ON exposure_recoveries (execution_id);

COMMIT;
