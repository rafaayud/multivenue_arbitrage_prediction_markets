BEGIN;

-- ---------------------------------------------------------------------------
-- Event projections: read models rebuilt from the durable binary journal.
-- Consumed by the Postgres projector and trading-activity APIs.
-- ---------------------------------------------------------------------------

-- Per-projector durability cursor: last journal sequence applied successfully.
CREATE TABLE IF NOT EXISTS journal_projection_checkpoints (
    projector_name TEXT PRIMARY KEY,
    last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Compact audit of projected journal entries for API/debug replay.
CREATE TABLE IF NOT EXISTS projected_events (
    journal_sequence BIGINT PRIMARY KEY,
    event_type TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    correlation_id TEXT,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS projected_events_type_sequence_idx
    ON projected_events (event_type, journal_sequence DESC);

-- Cross-venue matched contract pairs for a market cycle (underlying + interval).
CREATE TABLE IF NOT EXISTS matched_contracts (
    underlying TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
    left_contract_id TEXT NOT NULL,
    left_market_id TEXT NOT NULL,
    left_outcome_id TEXT NOT NULL,
    left_venue_id TEXT NOT NULL,
    left_symbol TEXT,
    right_contract_id TEXT NOT NULL,
    right_market_id TEXT NOT NULL,
    right_outcome_id TEXT NOT NULL,
    right_venue_id TEXT NOT NULL,
    right_symbol TEXT,
    ends_at TIMESTAMPTZ NOT NULL,
    -- Journal sequence that last wrote or refreshed this match.
    updated_sequence BIGINT NOT NULL,
    PRIMARY KEY (
        underlying,
        interval_seconds,
        left_contract_id,
        right_contract_id
    )
);

CREATE INDEX IF NOT EXISTS matched_contracts_cycle_idx
    ON matched_contracts (underlying, interval_seconds, ends_at);

-- Detected arbitrage opportunities projected from journal events.
CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
    opportunity_id TEXT PRIMARY KEY,
    -- One opportunity row per originating journal event.
    journal_sequence BIGINT NOT NULL UNIQUE,
    underlying TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    left_contract_id TEXT NOT NULL,
    right_contract_id TEXT NOT NULL,
    left_price NUMERIC NOT NULL CHECK (left_price >= 0 AND left_price <= 1),
    right_price NUMERIC NOT NULL CHECK (right_price >= 0 AND right_price <= 1),
    quantity NUMERIC NOT NULL CHECK (quantity > 0),
    gross_edge NUMERIC NOT NULL,
    net_edge NUMERIC NOT NULL,
    fee_per_contract NUMERIC NOT NULL,
    total_fees NUMERIC NOT NULL,
    -- Clock skew between venue books at detection time, in nanoseconds.
    skew_ns BIGINT NOT NULL CHECK (skew_ns >= 0),
    detected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS arbitrage_opportunities_cycle_idx
    ON arbitrage_opportunities (
        underlying,
        interval_seconds,
        journal_sequence DESC
    );

-- Projected order commands issued for an execution (primary vs hedge).
CREATE TABLE IF NOT EXISTS execution_order_commands (
    client_order_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('primary', 'hedge')),
    venue_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity NUMERIC NOT NULL CHECK (quantity > 0),
    limit_price NUMERIC,
    status TEXT NOT NULL,
    -- Journal sequences marking prepare / submit progress for this command.
    command_sequence BIGINT NOT NULL,
    prepared_sequence BIGINT,
    submitted_sequence BIGINT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS execution_order_commands_execution_idx
    ON execution_order_commands (execution_id, role);

-- ---------------------------------------------------------------------------
-- Monitor identity for recurring cycles and explicitly selected markets.
-- ---------------------------------------------------------------------------

ALTER TABLE matched_contracts
    ADD COLUMN IF NOT EXISTS monitor_type TEXT,
    ADD COLUMN IF NOT EXISTS monitor_key TEXT;

ALTER TABLE arbitrage_opportunities
    ADD COLUMN IF NOT EXISTS monitor_type TEXT,
    ADD COLUMN IF NOT EXISTS monitor_key TEXT;

UPDATE matched_contracts
SET monitor_type = 'cycle',
    monitor_key = 'cycle:' || underlying || ':' || interval_seconds
WHERE monitor_type IS NULL OR monitor_key IS NULL;

UPDATE arbitrage_opportunities
SET monitor_type = 'cycle',
    monitor_key = 'cycle:' || underlying || ':' || interval_seconds
WHERE monitor_type IS NULL OR monitor_key IS NULL;

ALTER TABLE matched_contracts
    ALTER COLUMN monitor_type SET NOT NULL,
    ALTER COLUMN monitor_key SET NOT NULL;

ALTER TABLE arbitrage_opportunities
    ALTER COLUMN monitor_type SET NOT NULL,
    ALTER COLUMN monitor_key SET NOT NULL;

DO $$
DECLARE
    primary_key_columns TEXT[];
BEGIN
    SELECT array_agg(attribute.attname ORDER BY key_column.ordinality)
    INTO primary_key_columns
    FROM pg_constraint AS constraint_row
    CROSS JOIN LATERAL unnest(constraint_row.conkey)
        WITH ORDINALITY AS key_column(attnum, ordinality)
    JOIN pg_attribute AS attribute
      ON attribute.attrelid = constraint_row.conrelid
     AND attribute.attnum = key_column.attnum
    WHERE constraint_row.conrelid = 'matched_contracts'::regclass
      AND constraint_row.contype = 'p';

    IF primary_key_columns IS DISTINCT FROM ARRAY[
        'monitor_key', 'left_contract_id', 'right_contract_id'
    ] THEN
        ALTER TABLE matched_contracts
            DROP CONSTRAINT IF EXISTS matched_contracts_pkey;
        ALTER TABLE matched_contracts
            ADD CONSTRAINT matched_contracts_pkey PRIMARY KEY (
                monitor_key,
                left_contract_id,
                right_contract_id
            );
    END IF;
END $$;

ALTER TABLE matched_contracts
    ALTER COLUMN underlying DROP NOT NULL,
    ALTER COLUMN interval_seconds DROP NOT NULL;

ALTER TABLE arbitrage_opportunities
    ALTER COLUMN underlying DROP NOT NULL,
    ALTER COLUMN interval_seconds DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'matched_contracts'::regclass
          AND conname = 'matched_contracts_monitor_type_check'
    ) THEN
        ALTER TABLE matched_contracts
            ADD CONSTRAINT matched_contracts_monitor_type_check
            CHECK (monitor_type IN ('cycle', 'regular'));
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'arbitrage_opportunities'::regclass
          AND conname = 'arbitrage_opportunities_monitor_type_check'
    ) THEN
        ALTER TABLE arbitrage_opportunities
            ADD CONSTRAINT arbitrage_opportunities_monitor_type_check
            CHECK (monitor_type IN ('cycle', 'regular'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS matched_contracts_monitor_idx
    ON matched_contracts (monitor_key, ends_at);

CREATE INDEX IF NOT EXISTS arbitrage_opportunities_monitor_idx
    ON arbitrage_opportunities (monitor_key, journal_sequence DESC);

COMMIT;
