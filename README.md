# Multi-venue Prediction Market Arbitrage

An event-driven Python trading engine with a React operations dashboard. This
snapshot focuses on cross-venue arbitrage in selected policy and political
events, including interest-rate decisions, across **Polymarket, Predict.fun and
Limitless**.

The project separates market semantics, trading decisions and venue integration
through domain models, ports and adapters. It includes order-book ingestion,
fee-aware detection, execution, recovery, persistence and operational metrics.

This is an engineering portfolio snapshot, not a hosted service or a guarantee of
profitable execution. It contains real order-submission code. Keep trading
disabled until credentials, contract equivalence and risk limits have been
reviewed.

## Strategy and scope

For equivalent binary markets, buying YES on one venue and NO on another can
provide a complementary payout. The basic long condition is:

```text
ask(YES, venue A) + ask(NO, venue B) + fees + cost buffer < 1
```

Covered short detection uses bids and requires inventory of both outcomes:

```text
bid(YES, venue A) + bid(NO, venue B) - fees - cost buffer > 1
```

The detector considers executable depth, tick sizes, book timing and configured
edge thresholds. The execution path adds admission, risk and freshness checks.
Orders on different venues are not atomic: one leg can fill while the other
fails. Recovery is a separate lifecycle and can require operator review.

These formulas assume matching resolution rules and compatible payout units.
Similar titles alone do not establish equivalence. Settlement disputes, fees,
collateral differences and venue availability remain operational risks.

The dashboard is focused on explicitly selected policy events. Recurring-market
adapters remain in the reusable codebase, but recurring monitoring is disabled
in the supplied Compose configuration. This is the two-leg cross-venue variant,
not the three-outcome football strategy.

## Architecture

```text
Venue REST / WebSocket adapters
             |
      normalized books
             v
   bounded event pipeline ---> journal / PostgreSQL projections
             |
     engine and domain rules
             |
     execution preparation
             v
   parallel venue dispatch ---> fills / recovery ---> engine

FastAPI control + trading services ---> React dashboard
                       |
                Prometheus metrics
```

| Location | Responsibility |
| --- | --- |
| `src/prediction_markets/domain/` | Contracts, order books, quantities, arbitrage rules and port contracts. |
| `src/prediction_markets/application/` | Engine state, bounded pipeline, admission, execution and recovery lifecycles. |
| `src/prediction_markets/infrastructure/` | Venue adapters, journals, database implementations and metrics. |
| `src/prediction_markets/api/` | FastAPI endpoints, runtime composition and trading controls. |
| `frontend/` | Event catalog, signals, orders, PnL, pipeline metrics and settings. |
| `migrations/` | PostgreSQL schema migrations. |
| `tests/` | Domain, application, adapter and API checks. |

Useful entry points for a code walkthrough:

- [Arbitrage rules](src/prediction_markets/domain/arbitrage/services.py)
- [Execution port](src/prediction_markets/domain/ports/execution.py)
- [Trading engine](src/prediction_markets/application/engine.py)
- [Pipeline composition](src/prediction_markets/application/pipeline/runtime.py)
- [Parallel order dispatch](src/prediction_markets/application/pipeline/order_dispatch.py)
- [Recovery decisions](src/prediction_markets/application/execution/recovery_decision.py)
- [Dashboard routes](frontend/src/app/routes.tsx)

The code exposes stage timings, event-loop lag and buffer metrics. This snapshot
does not claim a fixed end-to-end latency; measurements depend on feeds, hardware
and venue behavior. Market-data workers are configurable in the engine, but the
included event-dashboard deployment uses single-process market-data handling.

## Local setup

Requirements: Docker with Compose v2. For development outside Docker, use Python
3.13, `uv`, Node.js 22 and pnpm 11.

1. Copy `.env.example` to `.env` and supply your own configuration. In PowerShell:

   ```powershell
   Copy-Item .env.example .env
   ```

2. Configure the venue credentials you intend to use and a nonempty
   `TRADING_API_KEY`. Never put wallet keys or API secrets in `VITE_*` variables:
   frontend variables are public. Compose supplies its own local database DSN.
3. Start the local stack:

   ```sh
   docker compose up --build -d
   ```

4. Open the dashboard at <http://localhost:8080>. API documentation is available
   at <http://localhost:8000/docs>; Prometheus is at <http://localhost:9090>.
5. Select events, inspect book freshness and venue health, and review risk limits
   before explicitly enabling trading. The example file is not a funded demo
   account. Short inventory preparation may submit on-chain transactions.

The local ports bind to loopback. Database, journal and metrics data use new
Compose-managed volumes; no operational history is included. Running another
stack on the same ports will cause a conflict. Cloud deployment and private
alerting infrastructure are outside this snapshot.

To stop services without deleting their data:

```sh
docker compose down
```

## Development checks

```sh
uv sync --frozen --group dev
uv run pytest tests -m "not integration"

cd frontend
corepack pnpm install --frozen-lockfile
corepack pnpm typecheck
corepack pnpm test
corepack pnpm build
```

Tests marked `integration` contact external services and are excluded above.
Unit tests and frontend tests use test doubles; passing them does not establish
live execution safety. CI runs checks only and does not deploy or trade.

## Snapshot and attribution

This repository starts from a sanitized snapshot of `event-arbitrage-dashboard`.
It does not carry the original Git history, environment files, account data,
trade reports, local caches or private cloud configuration. Example environment
files deliberately contain no usable credentials. Original contributor
attribution is retained in `pyproject.toml`.
