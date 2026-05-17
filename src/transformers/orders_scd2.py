"""
orders_scd2.py — PySpark Structured Streaming: Debezium CDC events → Iceberg SCD Type 2

Data flow:
  Redpanda topic: ecommerce.public.orders
    → parse Debezium envelope  (before / after / op / source.lsn)
    → foreachBatch micro-batch
        Step 1: MERGE INTO — close active records for UPDATEs and DELETEs
        Step 2: INSERT     — open a new active record for INSERTs, UPDATEs, snapshots
    → Iceberg table: local.ecommerce.orders_scd2

SCD Type 2 means every change to a row creates a NEW record rather than overwriting.
The table accumulates history, and you query with `WHERE is_current = true` for live state
or filter on `effective_from / effective_to` to see any point-in-time snapshot.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ─── SPARK SESSION ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("orders-cdc-scd2")
    # Iceberg SQL extensions give us MERGE INTO and time-travel syntax
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Register a catalog named "local" backed by the local filesystem.
    # "hadoop" catalog type stores metadata alongside the Parquet data files
    # inside the warehouse directory — no external metastore needed.
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─── DEBEZIUM MESSAGE SCHEMA ──────────────────────────────────────────────────
# We only declare the fields we need. Extra fields in the JSON are silently ignored,
# which is intentional — this is our first layer of schema evolution tolerance.
# All fields are nullable because:
#   - `before` is null for INSERT events and snapshot reads  (op = c, r)
#   - `after`  is null for DELETE events                     (op = d)
order_row = StructType(
    [
        StructField("id",           IntegerType(), nullable=True),
        StructField("user_id",      IntegerType(), nullable=True),
        StructField("status",       StringType(),  nullable=True),
        StructField("total_amount", DoubleType(),  nullable=True),
        StructField("created_at",   StringType(),  nullable=True),
        StructField("updated_at",   StringType(),  nullable=True),
    ]
)

debezium_schema = StructType(
    [
        StructField("before", order_row,     nullable=True),
        StructField("after",  order_row,     nullable=True),
        StructField("op",     StringType(),  nullable=False),
        StructField("ts_ms",  LongType(),    nullable=False),  # wall-clock ms of the event
        StructField(
            "source",
            StructType(
                [
                    StructField("lsn",   LongType(), nullable=True),  # WAL position — used for dedup
                    StructField("ts_ms", LongType(), nullable=True),
                ]
            ),
            nullable=True,
        ),
    ]
)

# ─── BOOTSTRAP ICEBERG TABLE ──────────────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ecommerce")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.ecommerce.orders_scd2 (
        -- business columns (mirror of the Postgres orders table)
        id            INT,
        user_id       INT,
        status        STRING,
        total_amount  DOUBLE,
        created_at    TIMESTAMP,
        updated_at    TIMESTAMP,
        -- SCD Type 2 bookkeeping
        effective_from  TIMESTAMP,   -- when this row version became active
        effective_to    TIMESTAMP,   -- when it ended; NULL means still active
        is_current      BOOLEAN,
        -- CDC audit trail
        _op          STRING,         -- Debezium op code: r / c / u / d
        _source_lsn  LONG            -- Postgres WAL LSN for ordering and dedup
    ) USING iceberg
""")

# ─── READ STREAM ──────────────────────────────────────────────────────────────
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "ecommerce.public.orders")
    .option("startingOffsets", "earliest")  # replay full history on first run
    .option("failOnDataLoss", "false")
    .load()
)

events = raw_stream.select(
    from_json(col("value").cast("string"), debezium_schema).alias("d")
).select("d.*")


# ─── FOREACH BATCH: SCD TYPE 2 LOGIC ─────────────────────────────────────────
def apply_scd2(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Deduplicate within the batch.
    # If Spark replays a micro-batch (e.g. after a crash), duplicate LSNs would
    # create ghost records.  Dropping on LSN makes this function idempotent.
    # For multiple ops on the same id (rapid succession), we keep only the latest
    # via a window function so we never open two "active" records for one row.
    batch_df.createOrReplaceTempView("_cdc_raw")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW cdc_batch AS
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(after.id, before.id)
                    ORDER BY source.lsn DESC
                ) AS _rank
            FROM _cdc_raw
        )
        WHERE _rank = 1
    """)

    # ── Step 1: close the active record for any UPDATE or DELETE ──────────────
    # MERGE INTO finds the single row where is_current=true for that id and
    # sets effective_to + is_current=false. It's a no-op if no active record
    # exists yet (possible on replay).
    spark.sql("""
        MERGE INTO local.ecommerce.orders_scd2 AS t
        USING (
            SELECT
                COALESCE(after.id, before.id) AS id,
                to_timestamp(ts_ms / 1000)    AS event_ts
            FROM cdc_batch
            WHERE op IN ('u', 'd')
        ) AS s
        ON t.id = s.id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET
            t.is_current   = false,
            t.effective_to = s.event_ts
    """)

    # ── Step 2: insert the new active record for INSERTs, UPDATEs, snapshots ──
    # DELETEs are intentionally excluded — the closed record from step 1 is the
    # tombstone. No new "deleted" row is inserted; adjust if compliance rules
    # require an explicit deleted record.
    spark.sql("""
        INSERT INTO local.ecommerce.orders_scd2
        SELECT
            after.id,
            after.user_id,
            after.status,
            after.total_amount,
            CAST(after.created_at AS TIMESTAMP),
            CAST(after.updated_at AS TIMESTAMP),
            to_timestamp(ts_ms / 1000)   AS effective_from,
            CAST(null AS TIMESTAMP)      AS effective_to,
            true                         AS is_current,
            op                           AS _op,
            source.lsn                   AS _source_lsn
        FROM cdc_batch
        WHERE op IN ('r', 'c', 'u')
    """)


# ─── START ────────────────────────────────────────────────────────────────────
query = (
    events.writeStream.foreachBatch(apply_scd2)
    .option("checkpointLocation", "/warehouse/checkpoints/orders_scd2")
    .trigger(processingTime="15 seconds")
    .start()
)

print("Streaming started. Trigger interval: 15 seconds.")
print("Spark UI → http://localhost:4040")
query.awaitTermination()
