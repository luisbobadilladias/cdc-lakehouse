-- Full SCD Type 2 history for every order.
-- Each UPDATE produces two rows: the closed version (effective_to IS NOT NULL)
-- and the new active version (effective_to IS NULL).
-- A DELETE only closes the active row — no new row is inserted.
SELECT
    id,
    status,
    total_amount,
    is_current,
    effective_from,
    effective_to,
    _op,
    _source_lsn
FROM local.ecommerce.orders_scd2
ORDER BY id, effective_from;
