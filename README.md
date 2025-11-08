# NYC Taxi Streaming Pipeline

## Kafka Architecture

The stack runs Zookeeper, a single-broker Kafka node, and an init job that idempotently creates four topics: `nyctaxi.raw_trips`, `nyctaxi.clean_trips`, `nyctaxi.metrics_5m`, and `nyctaxi.dlq`.

Health checks guard during startup ordering and liveness; data is persisted on Docker Volumes; and the broker exposes the clients and inter-container ports for local development and integration testing.

## Data Flow

- Python Producer ingests raw taxi trip events and writes to `nyctaxi.raw_trips`. One or more processors consume from this topic, perform validation (schema/required fields), cleansing (type coersion, timestamp normalization), and light enrichment (lookups, derived columns) then publish clean events to `nyctaxi.clean_trips`.

- A windowed aggregator builds rolling five-minute KPIs (counts, fares, p95 trip time, error rates) and emits them to `nyctaxi.metrics_5m`

## Real World Applications

- Operational Monitoring: The `metrics_5m` topic drives alerts on ingestion lag, error rates, and anomalous volumes
- Feature Engineering: produce clean, time-aligned streams for model training or online features
