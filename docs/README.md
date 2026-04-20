
Project: Yelp Analytics Engine
This project involves building a multi-tier data pipeline to process 7 million Yelp records, moving from raw JSON blobs to a high-performance, validated SQL warehouse.

Phase 1: Bronze Layer (Raw Ingestion)
The goal was to get the raw data into a database as quickly as possible without losing any information.

Infrastructure: Set up a containerized PostgreSQL environment using Docker.

Storage Strategy: Used JSONB columns to store the original data. This keeps the source "immutable" so I can re-process it whenever the business logic changes.

Reliability: Implemented a row_hash system (SHA-256) to prevent duplicate entries and ensure the ingestion is idempotent.

![Broze Layer](<BRONZE LAYER.png>)

Phase 2: Silver Layer (Transformation & Quality)
I transformed the raw JSON into a structured Star Schema designed for heavy analytics.

1. Performance & Partitioning
Handling 6,990,280 (~7 Million) rows in one table is slow. I implemented Declarative Partitioning to split the fact_review table by year.

Result: The database now uses "Partition Pruning." If I query 2021 data, Postgres ignores the other 6 million rows entirely.

Benchmarking: Used EXPLAIN ANALYZE to prove that query times dropped from nearly a second to milliseconds.

![After Partion Performance](<Performance after Partitioning.png>)
![Before Indexing](<Before Indexing.png>)
![After Indexing](<AFTER INDEXING.png>)

2. Data Quality with Pydantic V2
I built a "Gatekeeper" layer using Pydantic to enforce a strict Data Contract.

Validation: Every record is checked for correct types and logical ranges (e.g., star ratings must be 0-5).

Quarantine Pattern: Malformed records aren't just dropped; they are sent to a bad_records table.

Finding: I successfully caught records with negative "useful" counts—a data quality issue in the source dataset that would have skewed my averages if left unchecked.

![Pydantic Transformation of Bad Records](<PYDANTIC TRANSFORMATION BAD RECORD.png>)

3. Final Schema
The Silver layer is now fully indexed and linked via foreign keys, ready for the Gold layer analytics.

![Silver Layer Final Image](<SILVER LAYER.png>)


Phase 3: Analytics Engineering (Gold Layer)
Transformed raw ingested data into high-value analytical marts using dbt Core.

Logic Implementation: Developed complex SQL models utilizing Window Functions (LAG) to calculate monthly growth metrics and Conditional Aggregations for business performance trends.

Data Integrity: Implemented a testing suite including unique, not_null, and relationship tests to ensure referential integrity between 7M reviews and business entities.

Performance Optimization: Materialized staging layers as Views to minimize storage overhead while materializing Marts as Tables for optimized query performance in downstream BI tools.

Lineage & Docs: Automated documentation generation showing the end-to-end flow from the silver (raw) schema to the analytics (gold) schema.

![Lineage Graph](<Lineage Graph.png>)

