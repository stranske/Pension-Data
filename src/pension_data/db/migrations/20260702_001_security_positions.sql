CREATE TABLE IF NOT EXISTS plan_security_positions (
    plan_id TEXT NOT NULL,
    plan_period TEXT NOT NULL,
    security_id TEXT NOT NULL,
    security_name TEXT,
    cusip TEXT,
    ticker TEXT,
    shares REAL,
    market_value_usd REAL,
    asset_class TEXT NOT NULL,
    source TEXT NOT NULL,
    as_of TEXT NOT NULL,
    disclosure_state TEXT NOT NULL,
    provenance_ref TEXT NOT NULL,
    manager_name TEXT,
    fund_name TEXT,
    confidence REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY (plan_id, plan_period, security_id, source, as_of, provenance_ref)
);

CREATE INDEX IF NOT EXISTS idx_plan_security_positions_plan_period
    ON plan_security_positions(plan_id, plan_period);

CREATE INDEX IF NOT EXISTS idx_plan_security_positions_asset_class
    ON plan_security_positions(plan_id, plan_period, asset_class);
