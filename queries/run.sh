#!/usr/bin/env bash
# Run an Iceberg SQL query against the Spark container.
# Usage: bash queries/run.sh 01_current_state.sql

set -euo pipefail

QUERY_FILE="${1:-01_current_state.sql}"

docker exec cdc_spark \
  /opt/spark/bin/spark-sql \
    --master "local[*]" \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/warehouse \
    --driver-java-options "-Divy.home=/root/.ivy2" \
    -f "/opt/spark/queries/${QUERY_FILE}"
