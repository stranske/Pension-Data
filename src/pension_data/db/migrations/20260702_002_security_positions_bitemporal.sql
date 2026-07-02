ALTER TABLE plan_security_positions
    ADD COLUMN valid_from TEXT;

ALTER TABLE plan_security_positions
    ADD COLUMN valid_to TEXT;

ALTER TABLE plan_security_positions
    ADD COLUMN asserted_at TEXT;

ALTER TABLE plan_security_positions
    ADD COLUMN superseded_at TEXT;

ALTER TABLE plan_security_positions
    ADD COLUMN amendment_accession TEXT;

UPDATE plan_security_positions
SET valid_from = as_of
WHERE valid_from IS NULL;

UPDATE plan_security_positions
SET asserted_at = as_of
WHERE asserted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_plan_security_positions_bitemporal
    ON plan_security_positions(plan_id, security_id, valid_from, asserted_at, superseded_at);
