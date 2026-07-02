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

CREATE TRIGGER IF NOT EXISTS trg_plan_security_positions_no_active_overlap_insert
BEFORE INSERT ON plan_security_positions
WHEN NEW.superseded_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'active valid-time overlap for plan_security_positions')
    WHERE EXISTS (
        SELECT 1
        FROM plan_security_positions existing
        WHERE existing.plan_id = NEW.plan_id
          AND existing.security_id = NEW.security_id
          AND existing.source = NEW.source
          AND existing.superseded_at IS NULL
          AND COALESCE(existing.valid_to, '9999-12-31T23:59:59Z') > NEW.valid_from
          AND COALESCE(NEW.valid_to, '9999-12-31T23:59:59Z') > existing.valid_from
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_plan_security_positions_no_active_overlap_update
BEFORE UPDATE ON plan_security_positions
WHEN NEW.superseded_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'active valid-time overlap for plan_security_positions')
    WHERE EXISTS (
        SELECT 1
        FROM plan_security_positions existing
        WHERE existing.rowid != OLD.rowid
          AND existing.plan_id = NEW.plan_id
          AND existing.security_id = NEW.security_id
          AND existing.source = NEW.source
          AND existing.superseded_at IS NULL
          AND COALESCE(existing.valid_to, '9999-12-31T23:59:59Z') > NEW.valid_from
          AND COALESCE(NEW.valid_to, '9999-12-31T23:59:59Z') > existing.valid_from
    );
END;
