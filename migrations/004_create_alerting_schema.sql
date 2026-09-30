BEGIN;

-- Correlated incident state materialized from durable journal events.
CREATE TABLE IF NOT EXISTS incidents (
    incident_id TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('open', 'acknowledged', 'in_progress', 'resolved', 'closed')
    ),
    severity TEXT NOT NULL CHECK (
        severity IN (
            'critical', 'major', 'minor', 'warning', 'informational', 'unknown'
        )
    ),
    source_component TEXT NOT NULL,
    source_service TEXT,
    source_instance TEXT,
    title TEXT,
    summary TEXT,
    description TEXT NOT NULL CHECK (BTRIM(description) != ''),
    opened_at TIMESTAMPTZ NOT NULL,
    acknowledged_at TIMESTAMPTZ,
    in_progress_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    CHECK (
        (status = 'open' AND acknowledged_at IS NULL
            AND in_progress_at IS NULL AND resolved_at IS NULL)
        OR (status = 'acknowledged' AND acknowledged_at IS NOT NULL
            AND in_progress_at IS NULL AND resolved_at IS NULL)
        OR (status = 'in_progress' AND acknowledged_at IS NOT NULL
            AND in_progress_at IS NOT NULL AND resolved_at IS NULL)
        OR (status IN ('resolved', 'closed') AND resolved_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS incidents_active_fingerprint_idx
    ON incidents (fingerprint)
    WHERE status NOT IN ('resolved', 'closed');

-- Transactional outbox. Alert fields are snapshots and never follow later
-- mutations of the incident row. Ephemeral operator notifications, such as
-- trade-completed, omit incident_id because they do not open an incident.
CREATE TABLE IF NOT EXISTS notification_deliveries (
    delivery_id TEXT PRIMARY KEY,
    incident_id TEXT,
    recipient_id TEXT NOT NULL,
    recipient_channel TEXT NOT NULL CHECK (
        recipient_channel IN ('email', 'voice', 'push', 'sms', 'other')
    ),
    recipient_address TEXT NOT NULL CHECK (BTRIM(recipient_address) != ''),
    alert_fingerprint TEXT NOT NULL,
    alert_state TEXT NOT NULL CHECK (alert_state IN ('firing', 'resolved')),
    alert_severity TEXT NOT NULL CHECK (
        alert_severity IN (
            'critical', 'major', 'minor', 'warning', 'informational', 'unknown'
        )
    ),
    source_component TEXT NOT NULL,
    source_service TEXT,
    source_instance TEXT,
    alert_title TEXT,
    alert_summary TEXT,
    alert_description TEXT NOT NULL CHECK (BTRIM(alert_description) != ''),
    alert_starts_at TIMESTAMPTZ NOT NULL,
    alert_ends_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'sending', 'delivered', 'failed')
    ),
    requested_at TIMESTAMPTZ NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    provider_reference TEXT,
    last_error TEXT,
    started_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    CHECK (
        (
            alert_state = 'firing'
            AND (
                alert_ends_at IS NULL
                OR alert_ends_at = alert_starts_at
            )
        )
        OR (alert_state = 'resolved' AND alert_ends_at IS NOT NULL)
    ),
    CHECK (
        (status = 'pending' AND started_at IS NULL AND delivered_at IS NULL
            AND failed_at IS NULL AND last_error IS NULL
            AND provider_reference IS NULL)
        OR (status = 'sending' AND attempt_count > 0 AND started_at IS NOT NULL
            AND delivered_at IS NULL AND failed_at IS NULL
            AND last_error IS NULL AND provider_reference IS NULL)
        OR (status = 'delivered' AND attempt_count > 0
            AND started_at IS NOT NULL AND delivered_at IS NOT NULL
            AND failed_at IS NULL AND last_error IS NULL)
        OR (status = 'failed' AND attempt_count > 0
            AND started_at IS NOT NULL AND failed_at IS NOT NULL
            AND delivered_at IS NULL AND last_error IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS notification_deliveries_claim_idx
    ON notification_deliveries (status, requested_at, delivery_id)
    WHERE status IN ('pending', 'sending');

CREATE INDEX IF NOT EXISTS notification_deliveries_incident_idx
    ON notification_deliveries (incident_id, requested_at)
    WHERE incident_id IS NOT NULL;

-- Bring existing databases in line when this migration is re-applied.
ALTER TABLE notification_deliveries
    DROP CONSTRAINT IF EXISTS notification_deliveries_incident_id_fkey;

ALTER TABLE notification_deliveries
    ALTER COLUMN incident_id DROP NOT NULL;

ALTER TABLE notification_deliveries
    DROP CONSTRAINT IF EXISTS notification_deliveries_check;

ALTER TABLE notification_deliveries
    DROP CONSTRAINT IF EXISTS notification_deliveries_alert_snapshot_check;

ALTER TABLE notification_deliveries
    ADD CONSTRAINT notification_deliveries_alert_snapshot_check
    CHECK (
        (
            alert_state = 'firing'
            AND (
                alert_ends_at IS NULL
                OR alert_ends_at = alert_starts_at
            )
        )
        OR (alert_state = 'resolved' AND alert_ends_at IS NOT NULL)
    );

COMMIT;
