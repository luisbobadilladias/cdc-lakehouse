# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Conventions

Use [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <description>`

Common types: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `ci`

## Project

A real-time CDC pipeline: PostgreSQL WAL → Debezium → Redpanda → PySpark → Apache Iceberg → Trino. Built as a sandbox for learning data engineering patterns (SCD Type 2, streaming ingestion, lakehouse time-travel queries).

## Infrastructure Commands

```bash
# Start the CDC pipeline (Phase 1 & 2)
docker compose up -d postgres redpanda redpanda-console kafka-connect

# Register Debezium connector (after Connect is healthy — ~30s)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json

# Check connector status
curl http://localhost:8083/connectors/ecommerce-postgres-connector/status | jq

# Start analytics layer (Phase 3 & 4)
docker compose --profile analytics up -d

# Tear everything down (keeps postgres_data volume)
docker compose down

# Full reset including data volume
docker compose down -v
```

## Architecture Data Flow

```
PostgreSQL (WAL logical replication)
  → Debezium (Kafka Connect) — emits {before, after, op} JSON per row change
  → Redpanda topic: ecommerce.public.<table>
  → PySpark Structured Streaming — SCD Type 2 MERGE INTO
  → Apache Iceberg (local Parquet + metadata in ./warehouse/)
  → Trino — time-travel SQL queries
```

## Key Config Files

- `config/postgres/init.sql` — schema, seed data, and the `dbz_publication` logical replication publication
- `config/debezium-connector.json` — Debezium connector payload; submit via `curl` to the Kafka Connect REST API at `:8083`
- `config/trino/` — Iceberg catalog config for Trino (Phase 4)

## Source Layout

- `src/producer/` — scripts to simulate continuous writes to PostgreSQL
- `src/transformers/` — PySpark Structured Streaming jobs
- `queries/` — Trino SQL scripts for validation and time-travel testing

## UIs

| Service           | URL                    |
|-------------------|------------------------|
| Redpanda Console  | http://localhost:8080  |
| Kafka Connect API | http://localhost:8083  |
| Spark UI          | http://localhost:4040  |
| Trino             | http://localhost:8090  |
