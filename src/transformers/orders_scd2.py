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
from pyspark.sql.functions import coalesce, col, desc, from_json, lit, row_number, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

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

    # Deduplicate: for rapid successive changes to the same id within one
    # micro-batch, keep only the event with the highest LSN.
    w = Window.partitionBy(
        coalesce(col("after.id"), col("before.id"))
    ).orderBy(desc("source.lsn"))

    deduped = (
        batch_df
        .withColumn("_rank", row_number().over(w))
        .filter(col("_rank") == 1)
        .drop("_rank")
    )

    # ── Step 1: close active records for UPDATEs and DELETEs ─────────────────
    # We use SQL UPDATE (not MERGE INTO) and collect the changed IDs to the
    # driver first.  This sidesteps the Iceberg SQL extension's inability to
    # resolve PySpark temp views across catalog boundaries.
    # Batch sizes in this pipeline are small (≤ rows changed per trigger), so
    # collecting to driver is safe.
    rows_to_close = (
        deduped
        .filter(col("op").isin("u", "d"))
        .select(
            coalesce(col("after.id"), col("before.id")).alias("id"),
            to_timestamp(col("ts_ms") / 1000).alias("event_ts"),
        )
        .collect()
    )

    for row in rows_to_close:
        spark.sql(f"""
            UPDATE local.ecommerce.orders_scd2
            SET    is_current   = false,
                   effective_to = TIMESTAMP '{row.event_ts}'
            WHERE  id = {row.id} AND is_current = true
        """)

    # ── Step 2: append new active records for INSERTs, UPDATEs, snapshots ────
    # DELETEs are excluded — the closed record from step 1 is the tombstone.
    new_records = (
        deduped
        .filter(col("op").isin("r", "c", "u"))
        .select(
            col("after.id").alias("id"),
            col("after.user_id").alias("user_id"),
            col("after.status").alias("status"),
            col("after.total_amount").alias("total_amount"),
            col("after.created_at").cast("timestamp").alias("created_at"),
            col("after.updated_at").cast("timestamp").alias("updated_at"),
            to_timestamp(col("ts_ms") / 1000).alias("effective_from"),
            lit(None).cast("timestamp").alias("effective_to"),
            lit(True).alias("is_current"),
            col("op").alias("_op"),
            col("source.lsn").alias("_source_lsn"),
        )
    )

    new_records.writeTo("local.ecommerce.orders_scd2").append()


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
