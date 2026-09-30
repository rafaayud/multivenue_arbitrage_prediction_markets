BEGIN;

ALTER TABLE pnl_performance_points
    ADD COLUMN IF NOT EXISTS trading_fees_usd NUMERIC;

-- The activity stream only reads the latest bot point. Seed that point once;
-- future points receive the value directly from the journal projector.
WITH latest_bot_point AS (
    SELECT point_id
    FROM pnl_performance_points
    WHERE source = 'bot_ledger' AND venue_id IS NULL
    ORDER BY observed_at DESC, point_id DESC
    LIMIT 1
)
UPDATE pnl_performance_points AS point
SET trading_fees_usd = COALESCE((
    SELECT SUM(trade.fee_settlement_amount)
    FROM trades AS trade
    WHERE trade.portfolio_id = 'cross-venue-arbitrage'
      AND trade.fee_settlement_currency = 'USD'
      AND trade.executed_at <= point.observed_at
), 0)
FROM latest_bot_point AS latest
WHERE point.point_id = latest.point_id
  AND point.trading_fees_usd IS NULL;

COMMIT;
