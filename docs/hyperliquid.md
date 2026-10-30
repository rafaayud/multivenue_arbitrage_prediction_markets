# Hyperliquid YES/NO market data

This integration reads native Hyperliquid outcome contracts. It is **read-only**:
there is no order execution, inventory mutation, fee calculator or automatic
cross-venue matching. The runtime can monitor its public books for explicitly
selected mainnet events, without generating Hyperliquid arbitrage signals.
The existing trading routes remain limited to Polymarket, Predict.fun and Limitless.

## Enable the explorer

Set these values in both the control API and trading service environments, then
rebuild/restart both services and the frontend with the updated code:

```dotenv
HYPERLIQUID_ENABLED=true
HYPERLIQUID_NETWORK=mainnet
```

Use `testnet` to select the test network. No Hyperliquid wallet or API key is
required. The integration is disabled by default. This switch enables public
data access only and cannot enable trading.

Open **Event catalog → Hyperliquid outcomes**, search for a question and select
it to inspect both books. The catalog includes listed USDC-denominated contracts
with explicit YES/NO labels and a future metadata deadline. Participant-vs-participant,
Over/Under and fallback outcomes are excluded. Questions with unknown deadlines
are available through the API with `active_only=false`.

The selected books refresh through REST every five seconds after each completed
request. The display shows venue timestamps separately from fetch time, clears
books after failed requests, and stops polling when the selection is closed.
The two books are independent snapshots; fetching them together does not make
their timestamps identical. The UI displays ten levels per side; the HTTP API
returns up to twenty.

The main catalog's **Monitor** button also includes Hyperliquid when AGG provides
an explicit `outcome:<id>` identifier. The runtime resolves that mainnet id and
subscribes to both native YES and NO books over WebSocket. Testnet is available
only through the separate explorer because AGG identifiers refer to mainnet.
The dashboard labels Hyperliquid **Books only**. In a mixed event, supported
Polymarket/Predict/Limitless pairs retain their signals and inventory controls;
pairs involving Hyperliquid cannot generate signals, place orders or mutate inventory.
Read-only venues do not participate in live-trading collateral or health checks.

## Public API and stream adapter

- `GET /hyperliquid/markets?search_text=Fed&limit=100`: catalog, network and
  explicit `trading_enabled: false`. A disabled integration returns an empty
  catalog with `enabled: false`.
- `GET /hyperliquid/markets/{outcome_id}/books`: native YES and NO snapshots.
  Disabled access returns 503, an unsupported/unlisted id returns 404 and an
  upstream read or normalization failure returns 502.

Through the dashboard proxy, prepend `/api` to these paths.

The Python adapters implement the existing discovery, REST market-data,
WebSocket market-data and venue-health ports. Metadata and templates share a
30-second cache. Parent question titles and resolution rules remain attached
to named outcomes such as a policy-rate increase.

The stream adapter uses native `l2Book` subscriptions, a bounded receive queue,
application pings and reconnect backoff. It invalidates subscribed books after
disconnects and distinguishes initial snapshots from subsequent updates. Its
transport timestamps and queue metrics use the existing instrumentation. The
runtime uses this stream for monitored events; the separate explorer uses REST polling.

Run a public metadata/health/WebSocket check without starting the trading runtime:

```sh
uv run python -m prediction_markets.infrastructure.venues.hyperliquid --network mainnet
# Add --market <outcome_id> to inspect a specific active YES/NO question.
```

## Protocol boundaries and trading follow-up

Native encoding is `10 * outcome_id + side`, with YES=0 and NO=1. The adapter's
contract id is `hyperliquid:{outcome_id}:yes|no`; the book coin is `#encoding`.
These are distinct from ordinary spot instrument indexes. YES and NO prices
are read from their native books rather than synthesized with `1 - price`.
See Hyperliquid's [asset identifiers](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids)
and [outcome metadata API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot).

Trading remains blocked pending verification of the actual outcome fee rules.
The [outcome overview](https://hyperliquid.gitbook.io/hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-4-outcome-markets)
mentions zero fees for initial testing, while the current
[fees page](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees)
describes charges on closing and settlement. This implementation does not infer
zero fees from that inconsistency and does not produce executable arbitrage
signals for Hyperliquid.

Before adding execution, verify fee denomination and settlement charges,
order-size constraints, durable submission/reconciliation and manual inventory
accounting on the chosen network. The current contract metadata uses a price
tick of `0.00001` and whole-token lots, consistent with the
[Nautilus integration](https://github.com/nautechsystems/nautilus_trader/blob/develop/docs/integrations/hyperliquid.md).
No live orders or inventory operations are part of the data check above.

## Live frontend check

With the read-only integration enabled in the local application, run this from
`frontend/` in PowerShell:

```powershell
$env:HYPERLIQUID_LIVE_BASE_URL = 'http://localhost:8080'
npm.cmd test -- src/components/candidates/hyperliquid-outcomes.live.test.tsx
```

This opt-in test renders the catalog component in JSDOM and reads real data
through the deployed `/api` proxy. It searches for the Fed rate-increase outcome,
checks displayed YES/NO bid/ask prices and sizes against the actual API response,
and waits for a subsequent poll. It sends no trading requests. The check requires
that outcome to remain listed with liquidity; it is skipped in ordinary unit
test runs when `HYPERLIQUID_LIVE_BASE_URL` is unset. It does not replace visual
browser inspection.
