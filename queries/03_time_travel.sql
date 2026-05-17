-- ─── STEP 1: list available snapshots ────────────────────────────────────────
-- Every micro-batch trigger that writes data creates a new Iceberg snapshot.
-- Each snapshot is a complete, immutable version of the table at that moment.
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-records']    AS records_added,
    summary['changed-records']  AS records_changed
FROM local.ecommerce.orders_scd2.snapshots
ORDER BY committed_at;

-- ─── STEP 2: time travel by snapshot ID ──────────────────────────────────────
-- Copy a snapshot_id from the output above (pick one before your most recent UPDATE).
-- Replace <snapshot_id> with the actual integer value.
--
-- SELECT id, status, is_current
-- FROM local.ecommerce.orders_scd2 VERSION AS OF <snapshot_id>
-- WHERE is_current = true
-- ORDER BY id;

-- ─── STEP 3: time travel by timestamp ────────────────────────────────────────
-- Replace the timestamp with a moment from before your most recent UPDATE.
-- Format: 'YYYY-MM-DD HH:MM:SS'
--
-- SELECT id, status, is_current
-- FROM local.ecommerce.orders_scd2 TIMESTAMP AS OF '2025-01-01 12:00:00'
-- WHERE is_current = true
-- ORDER BY id;
