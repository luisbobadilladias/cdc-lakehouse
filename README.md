# CDC Lakehouse

A real-time Change Data Capture pipeline that streams PostgreSQL row-level changes into an Apache Iceberg lakehouse, preserving full change history (SCD Type 2) for analytics and ML.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  PostgreSQL 15                                           │
│  wal_level = logical  ←  every row change is recorded   │
└──────────────────┬───────────────────────────────────────┘
                   │  replication slot (WAL stream)
                   ▼
┌──────────────────────────────────────────────────────────┐
│  Debezium 2.7  (inside Kafka Connect)                    │
│  Reads the WAL, emits { before, after, op } per change   │
└──────────────────┬───────────────────────────────────────┘
                   │  JSON events
                   ▼
┌──────────────────────────────────────────────────────────┐
│  Redpanda  (Kafka-compatible broker)                     │
│  Topics: ecommerce.public.users                         │
│          ecommerce.public.orders                        │
└──────────────────┬───────────────────────────────────────┘
                   │  Kafka consumer
                   ▼
┌──────────────────────────────────────────────────────────┐
│  PySpark Structured Streaming                            │
│  SCD Type 2 MERGE INTO → Apache Iceberg tables          │
└──────────────────┬───────────────────────────────────────┘
                   │  Parquet + Iceberg metadata (local)
                   ▼
┌──────────────────────────────────────────────────────────┐
│  Trino  ←  SQL time-travel queries over Iceberg         │
└──────────────────────────────────────────────────────────┘
```

## Stack

| Component     | Technology            | Why                                                         |
|---------------|-----------------------|-------------------------------------------------------------|
| Source DB     | PostgreSQL 15         | Logical replication (WAL) built-in                         |
| CDC Engine    | Debezium 2.7          | Reads WAL asynchronously — zero query load on source DB    |
| Event Broker  | Redpanda              | Kafka-compatible, no Zookeeper, single-process local setup |
| Processing    | PySpark 3.5           | Structured Streaming + MERGE INTO for SCD Type 2           |
| Lakehouse     | Apache Iceberg        | ACID, time travel, schema evolution, vendor-neutral        |
| Query Engine  | Trino 435             | Fast SQL over Iceberg without loading data into memory     |

## Phases

| Phase | What gets built                                |
|-------|------------------------------------------------|
| 1     | Infrastructure (Postgres, Redpanda, Debezium)  |
| 2     | Streaming ingestion — inspect live CDC events  |
| 3     | PySpark transformer + SCD Type 2 Iceberg write |
| 4     | Trino queries + time-travel verification       |

---

## Phase 1: Start the CDC Pipeline

```bash
docker compose up -d postgres redpanda redpanda-console kafka-connect
```

Wait ~30 seconds for Kafka Connect to finish initialising, then register Debezium:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json
```

Check connector status:

```bash
curl http://localhost:8083/connectors/ecommerce-postgres-connector/status | jq
```

Open **Redpanda Console** at http://localhost:8080 — you should see the `ecommerce.public.orders` and `ecommerce.public.users` topics already populated from the initial snapshot.

Trigger a change and watch it appear in the topic:

```bash
docker exec -it cdc_postgres psql -U postgres -d ecommerce \
  -c "UPDATE orders SET status = 'shipped' WHERE id = 2;"
```

---

## Design Trade-offs

**Redpanda vs standard Kafka**: Redpanda runs as a single binary with no Zookeeper dependency. The Kafka API compatibility means zero code changes when switching to a managed Kafka service in production (Confluent Cloud, MSK, Aiven).

**Apache Iceberg vs Delta Lake**: Iceberg is vendor-neutral — Spark, Flink, Trino, Hive, and Presto all support it natively via the open table spec. Delta Lake has tighter Databricks integration and is the better choice if you're already in that ecosystem.

**WAL tailing vs polling**: Debezium reads PostgreSQL's internal replication stream rather than running `SELECT` queries. This means zero additional load on your source database, even under high write rates.

**`snapshot.mode = initial`**: When the connector first starts it performs a one-time consistent snapshot of all existing rows, then switches to streaming new WAL events. This is how seed data lands in Kafka.
