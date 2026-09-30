BEGIN;

CREATE TABLE IF NOT EXISTS binary_contract_complements (
    venue_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    complement_contract_id TEXT NOT NULL,
    PRIMARY KEY (venue_id, contract_id),
    CHECK (contract_id <> complement_contract_id)
);

WITH market_contracts AS (
    SELECT left_venue_id AS venue_id,
           left_market_id AS market_id,
           left_contract_id AS contract_id
    FROM matched_contracts
    UNION
    SELECT right_venue_id,
           right_market_id,
           right_contract_id
    FROM matched_contracts
), binary_markets AS (
    SELECT venue_id, market_id
    FROM market_contracts
    GROUP BY venue_id, market_id
    HAVING COUNT(*) = 2
), complements AS (
    SELECT contract.venue_id,
           contract.market_id,
           contract.contract_id,
           complement.contract_id AS complement_contract_id
    FROM market_contracts AS contract
    JOIN binary_markets AS market
      ON market.venue_id = contract.venue_id
     AND market.market_id = contract.market_id
    JOIN market_contracts AS complement
      ON complement.venue_id = contract.venue_id
     AND complement.market_id = contract.market_id
     AND complement.contract_id <> contract.contract_id
)
INSERT INTO binary_contract_complements (
    venue_id, market_id, contract_id, complement_contract_id
)
SELECT venue_id, market_id, contract_id, complement_contract_id
FROM complements
ON CONFLICT (venue_id, contract_id) DO UPDATE SET
    market_id = EXCLUDED.market_id,
    complement_contract_id = EXCLUDED.complement_contract_id;

WITH position_contracts AS (
    SELECT DISTINCT
           venue_id,
           split_part(contract_id, ':', 2) AS market_id,
           contract_id
    FROM positions
    WHERE split_part(contract_id, ':', 2) <> ''
), binary_markets AS (
    SELECT venue_id, market_id
    FROM position_contracts
    GROUP BY venue_id, market_id
    HAVING COUNT(*) = 2
), complements AS (
    SELECT contract.venue_id,
           contract.market_id,
           contract.contract_id,
           complement.contract_id AS complement_contract_id
    FROM position_contracts AS contract
    JOIN binary_markets AS market
      ON market.venue_id = contract.venue_id
     AND market.market_id = contract.market_id
    JOIN position_contracts AS complement
      ON complement.venue_id = contract.venue_id
     AND complement.market_id = contract.market_id
     AND complement.contract_id <> contract.contract_id
)
INSERT INTO binary_contract_complements (
    venue_id, market_id, contract_id, complement_contract_id
)
SELECT venue_id, market_id, contract_id, complement_contract_id
FROM complements
ON CONFLICT (venue_id, contract_id) DO UPDATE SET
    market_id = EXCLUDED.market_id,
    complement_contract_id = EXCLUDED.complement_contract_id;

COMMIT;
