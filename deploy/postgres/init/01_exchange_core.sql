CREATE SCHEMA IF NOT EXISTS exchange_core;

CREATE TABLE IF NOT EXISTS exchange_core.strategy_template
(
    id            BIGSERIAL PRIMARY KEY,
    template_code TEXT NOT NULL UNIQUE,
    template_name TEXT NOT NULL,
    bucket        TEXT NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    description   TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS exchange_core.strategy_param_version
(
    id            BIGSERIAL PRIMARY KEY,
    template_code TEXT NOT NULL REFERENCES exchange_core.strategy_template(template_code),
    version       INTEGER NOT NULL,
    params_json   JSONB NOT NULL,
    published_by  TEXT NOT NULL DEFAULT '',
    published_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    remark        TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_strategy_param_version UNIQUE (template_code, version)
);

CREATE INDEX IF NOT EXISTS idx_strategy_param_version_template
    ON exchange_core.strategy_param_version (template_code, version DESC);

CREATE TABLE IF NOT EXISTS exchange_core.orders
(
    id                BIGSERIAL PRIMARY KEY,
    account_id        TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    order_id          TEXT NOT NULL,
    client_order_id   TEXT NOT NULL DEFAULT '',
    strategy_id       TEXT NOT NULL DEFAULT '',
    position_cycle_id TEXT NOT NULL DEFAULT '',
    side              TEXT NOT NULL,
    type              TEXT NOT NULL,
    price             NUMERIC(20, 8) NOT NULL DEFAULT 0,
    quantity          NUMERIC(20, 8) NOT NULL DEFAULT 0,
    executed_quantity NUMERIC(20, 8) NOT NULL DEFAULT 0,
    status            TEXT NOT NULL,
    reduce_only       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_orders_account_order UNIQUE (account_id, order_id),
    CONSTRAINT uq_orders_account_client_order UNIQUE (account_id, client_order_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol_status
    ON exchange_core.orders (symbol, status);

CREATE INDEX IF NOT EXISTS idx_orders_cycle
    ON exchange_core.orders (position_cycle_id);

CREATE TABLE IF NOT EXISTS exchange_core.trades
(
    id           BIGSERIAL PRIMARY KEY,
    account_id   TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    trade_id     TEXT NOT NULL,
    order_id     TEXT NOT NULL,
    price        NUMERIC(20, 8) NOT NULL,
    qty          NUMERIC(20, 8) NOT NULL,
    fee          NUMERIC(20, 8) NOT NULL DEFAULT 0,
    fee_asset    TEXT NOT NULL DEFAULT '',
    realized_pnl NUMERIC(20, 8) NOT NULL DEFAULT 0,
    trade_time   TIMESTAMPTZ NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_trades_account_trade UNIQUE (account_id, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_order_id
    ON exchange_core.trades (order_id);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
    ON exchange_core.trades (symbol, trade_time DESC);

CREATE TABLE IF NOT EXISTS exchange_core.positions
(
    id             BIGSERIAL PRIMARY KEY,
    account_id     TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    position_side  TEXT NOT NULL,
    position_amt   NUMERIC(20, 8) NOT NULL DEFAULT 0,
    entry_price    NUMERIC(20, 8) NOT NULL DEFAULT 0,
    mark_price     NUMERIC(20, 8) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(20, 8) NOT NULL DEFAULT 0,
    leverage       NUMERIC(10, 2) NOT NULL DEFAULT 1,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_positions_account_symbol_side UNIQUE (account_id, symbol, position_side)
);

CREATE INDEX IF NOT EXISTS idx_positions_symbol
    ON exchange_core.positions (symbol);

CREATE TABLE IF NOT EXISTS exchange_core.api_audit_log
(
    id                BIGSERIAL PRIMARY KEY,
    request_id        TEXT NOT NULL DEFAULT '',
    user_id           TEXT NOT NULL DEFAULT '',
    account_id        TEXT NOT NULL DEFAULT '',
    path              TEXT NOT NULL,
    method            TEXT NOT NULL,
    action            TEXT NOT NULL DEFAULT '',
    request_body_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_code     INTEGER NOT NULL DEFAULT 0,
    ip                TEXT NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_audit_log_request_id
    ON exchange_core.api_audit_log (request_id);

CREATE INDEX IF NOT EXISTS idx_api_audit_log_created_at
    ON exchange_core.api_audit_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_api_audit_log_account_id
    ON exchange_core.api_audit_log (account_id);