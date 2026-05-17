-- Current state of all orders.
-- Equivalent to a plain SELECT * on the source Postgres table,
-- but served entirely from the Iceberg lakehouse.
SELECT
    id,
    user_id,
    status,
    total_amount,
    effective_from,
    _op
FROM local.ecommerce.orders_scd2
WHERE is_current = true
ORDER BY id;
