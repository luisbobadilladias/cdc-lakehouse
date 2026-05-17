#!/usr/bin/env bash
# Submit orders_scd2.py to the Spark container.
#
# First run downloads ~200 MB of JARs into the spark_ivy volume;
# subsequent runs are instant because the volume is reused.
#
# Packages:
#   iceberg-spark-runtime  — Iceberg catalog + MERGE INTO support for Spark 3.5
#   spark-sql-kafka        — Structured Streaming Kafka source

set -euo pipefail

docker exec cdc_spark spark-submit \
  --master "local[*]" \
  --packages \
    org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.driver.extraJavaOptions="-Divy.home=/root/.ivy2" \
  /opt/bitnami/spark/work/orders_scd2.py
