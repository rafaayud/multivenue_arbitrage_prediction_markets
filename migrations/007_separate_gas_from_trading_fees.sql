BEGIN;

ALTER TABLE pnl_performance_points
    ADD COLUMN IF NOT EXISTS gas_usd NUMERIC;

-- Historical bot points stored every USD cost in fees_usd. Trades retain
-- their own fees, so the remainder is inventory-operation gas.
UPDATE pnl_performance_points AS point
SET gas_usd = GREATEST(
    COALESCE(point.fees_usd, 0)
    - COALESCE((
        SELECT SUM(trade.fee_settlement_amount)
        FROM trades AS trade
        WHERE trade.portfolio_id = 'cross-venue-arbitrage'
          AND trade.fee_settlement_currency = 'USD'
          AND trade.executed_at <= point.observed_at
    ), 0)
    - COALESCE((
        SELECT SUM(movement.fee_amount)
        FROM cash_movements AS movement
        WHERE movement.fee_currency = 'USD'
          AND movement.occurred_at <= point.observed_at
          AND (
              movement.source_portfolio_id = 'cross-venue-arbitrage'
              OR movement.destination_portfolio_id = 'cross-venue-arbitrage'
          )
    ), 0),
    0
)
WHERE point.source = 'bot_ledger'
  AND point.fees_usd IS NOT NULL;

COMMIT;
