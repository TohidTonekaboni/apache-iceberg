# Apache Iceberg — Practical Learning Roadmap

A hands-on, project-based path to deeply learning **Apache Iceberg** as a table
format and **Apache Polaris** (plus the **Iceberg REST Catalog**) as catalog
implementations — using **Spark**, **Trino**, and **Dremio** as the three query engines.

Every phase has three layers, do them in order and don't skip layer 2/3 just
because layer 1 "works":

- 🛠 **Practical** — something you run and inspect in this repo.
- 🔬 **Internals** — the file format / protocol you should be able to explain on a whiteboard.
- 📖 **Spec / source** — the primary source to actually read, not a blog post about it.

Environment lives in [docker-compose.yml](docker-compose.yml). We'll grow it
phase by phase (Spark + REST catalog + MinIO exist already; Trino and Dremio
get added in Phase 3, Polaris gets added in Phase 5).

---

## Phase 0 — Environment & Mental Model

**Goal:** get the stack running and understand the four things Iceberg separates
that Hive smashed together: *table format*, *file format*, *catalog*, *compute engine*.

- 🛠
  - `docker compose up -d` and confirm `minio`, `rest`, `spark-iceberg` are healthy.
  - Open MinIO console (`localhost:9001`, admin/password) — watch the empty `warehouse` bucket.
  - Open the Spark notebook UI (`localhost:8888`) and run `spark.sql("SHOW NAMESPACES").show()`.
- 🔬 Draw the stack for yourself: Client (Spark/Trino/Dremio) → Catalog (REST/Polaris) → Table Metadata (S3/MinIO) → Data Files (Parquet, on S3/MinIO). Identify which piece each existing docker service plays.
- 📖 Read the Iceberg docs overview: https://iceberg.apache.org/docs/latest/ and the "Why Iceberg" motivation section — specifically what problem it solves vs. Hive tables (no atomic commits, partition = physical layout, no schema evolution safety).

---

## Phase 1 — Core Iceberg Table Operations (Spark)

**Goal:** create, write, read, and evolve a table using only SQL — no internals yet, just build correct muscle memory.

- 🛠
  - `CREATE TABLE demo.db.orders (...) USING iceberg PARTITIONED BY (days(order_ts))`.
  - Insert data in 3 separate batches (`INSERT INTO ... VALUES ...`, 3 times) so you generate 3 snapshots.
  - `SELECT * FROM demo.db.orders.snapshots` and `demo.db.orders.history` — note snapshot IDs and timestamps.
  - `ALTER TABLE ... ADD COLUMN`, then `ALTER TABLE ... ADD PARTITION FIELD` (partition evolution without rewriting data).
  - Time travel: `SELECT * FROM demo.db.orders VERSION AS OF <snapshot_id>` and `... TIMESTAMP AS OF ...`.
  - `CALL demo.system.rollback_to_snapshot('db.orders', <snapshot_id>)`.
- 🔬 Understand: a table is not a folder of files — it's a **pointer to a metadata.json file**, and every DDL/DML is a new metadata.json plus a catalog commit that atomically swaps the pointer. This is why Iceberg gets ACID without a lock manager: the catalog does one compare-and-swap.
- 📖 Read the Iceberg Table Spec sections "Overview" and "Snapshots": https://iceberg.apache.org/spec/

---

## Phase 2 — Metadata Internals (the deep-dive phase)

**Goal:** stop treating Iceberg as a black box. Open the actual files it wrote in MinIO and match them to the spec.

- 🛠
  - `mc` (already in compose) or the MinIO console: browse `s3://warehouse/db/orders/metadata/` and `.../data/`.
  - Download and `cat`/`jq` the latest `v<N>.metadata.json` — identify `current-snapshot-id`, `snapshots[]`, `schemas[]`, `partition-specs[]`.
  - Find the `snap-<id>-...avro` manifest list for the current snapshot; use `pyiceberg` or `java -jar avro-tools` (or a quick PySpark `spark.read.format("avro")`) to dump it — see the list of **manifest files** with `added_files_count` / `existing_files_count` / `deleted_files_count`.
  - Open one **manifest file** (also Avro) — see per-data-file stats: `file_path`, `partition`, `record_count`, `column_sizes`, `value_counts`, `lower_bounds`/`upper_bounds` (this is what powers file pruning).
  - Trace one full chain by hand: `metadata.json → manifest-list.avro → manifest-*.avro → data-*.parquet`, writing down every ID that links them.
- 🔬 Concepts to nail down: snapshot isolation, manifest partitioning (why manifests get split/merged), min/max stats pruning, why `metadata.json` grows and needs `expire_snapshots`, sequence numbers vs snapshot IDs (used for row-level delete ordering).
- 📖 Read the full spec's "Manifests", "Manifest Lists", and "Table Metadata" sections. Skim `org.apache.iceberg.ManifestReader` / `TableMetadata` in the Iceberg source (https://github.com/apache/iceberg) to see the Java model mirrors the spec 1:1.

---

## Phase 3 — Multi-Engine Interoperability (add Trino & Dremio)

**Goal:** prove Iceberg's core value prop — the *same table*, same catalog, read/written correctly from three different engines.

- 🛠
  - Add a `trino` service to `docker-compose.yml` pointing its Iceberg connector at the same REST catalog (`iceberg.rest-catalog.uri=http://rest:8181`) and MinIO warehouse.
  - Add a `dremio` service (official `dremio/dremio-oss` image) and configure an Iceberg REST source pointing at the same catalog/warehouse (Dremio's "Iceberg REST Catalog" or "Nessie/Arctic" source type, plus its S3 source config for MinIO).
  - From Trino: `SELECT * FROM iceberg.db.orders` — same rows Spark wrote. From Dremio's SQL editor: the same query against the same table via its added source.
  - Write from Trino (`INSERT INTO ...`), then re-read from Spark and Dremio and confirm the new snapshot shows up in `.history`.
  - In Dremio, build a **reflection** (Dremio's materialized-view/query-acceleration feature) on top of the table and compare query plans/latency with and without it — this is the one feature the other two engines don't have.
  - Force a conflict: start a long write in Spark, concurrently write from Trino or Dremio, observe optimistic-concurrency retry/failure behavior in the catalog commit.
- 🔬 Understand there is no engine-specific metadata — the metadata.json/manifests are engine-agnostic, so interoperability is a property of the **format + catalog protocol**, not connector code. Compare how each engine's Iceberg connector implements planning (all three must do manifest pruning independently, per the same stats) and note that Dremio adds a layer on top (reflections, semantic layer/virtual datasets) that sits outside the Iceberg spec entirely.
- 📖 Trino Iceberg connector docs (https://trino.io/docs/current/connector/iceberg.html) — read the "Concurrent writes" and "Table properties" sections specifically. Dremio docs on Iceberg tables and reflections: https://docs.dremio.com/current/reference/sql/commands/apache-iceberg-tables/ and https://docs.dremio.com/current/sonar/reflections/.

---

## Phase 4 — The Iceberg REST Catalog Protocol

**Goal:** understand the catalog as a *protocol*, not a product — this is the piece that unifies Polaris, Nessie, Snowflake, Tabular, Glue, etc.

- 🛠
  - Hit the REST catalog directly with `curl` (bypass Spark/Trino entirely): `GET http://localhost:8181/v1/config`, `GET /v1/namespaces`, `GET /v1/namespaces/db/tables/orders`.
  - Diff what the REST catalog returns for `metadata-location` against what you found by hand in Phase 2.
  - Manually perform a **commit** via the REST API's `POST /v1/namespaces/{ns}/tables/{table}` update-table request with a `requirement`/`update` pair — see the compare-and-swap contract explicitly (this *is* the transaction primitive).
- 🔬 The catalog's only real jobs: (1) map table name → current metadata location, (2) atomic compare-and-swap on that pointer, (3) optionally vend short-lived, scoped storage credentials. Everything else (query planning, file pruning) happens in the engine, not the catalog.
- 📖 Read the REST Catalog Open API spec: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml — focus on `LoadTableResult`, `CommitTableRequest`, and `TableRequirement`.

---

## Phase 5 — Apache Polaris as a Catalog

**Goal:** run Polaris for real, understand it's a REST-catalog-protocol implementation *plus* multi-tenant governance (principals, roles, catalog-level RBAC, credential vending) that the bare REST fixture doesn't have.

- 🛠
  - Add a `polaris` service to `docker-compose.yml` (official image `apache/polaris`), backed by the same MinIO warehouse (separate prefix, e.g. `s3://warehouse/polaris/`).
  - Bootstrap: create a root principal/credentials, create a **catalog**, a **principal role**, a **catalog role**, and grant privileges (`CATALOG_MANAGE_CONTENT`, `TABLE_READ_DATA`, etc.) — do this via Polaris's own REST management API with `curl`, not just the CLI, so you see the objects being created.
  - Point Spark's Iceberg catalog config at Polaris instead of the REST fixture (`spark.sql.catalog.polaris.uri`, `credential`, `scope=PRINCIPAL_ROLE:ALL`). Recreate the Phase 1 table under Polaris.
  - Point Trino and Dremio at Polaris too — confirm the same multi-engine interop from Phase 3 now works through Polaris for all three engines.
  - Create a second, restricted principal (read-only catalog role) and prove it can `SELECT` but not `INSERT`/`DROP` from each of Spark, Trino, and Dremio.
  - Inspect the **vended credentials**: capture the temporary S3 STS-style credentials Polaris hands to the engine per-request and note their scope/expiry vs. the static `admin/password` MinIO keys used by the plain REST fixture.
- 🔬 Polaris = REST Catalog protocol (Phase 4) + an authorization/governance layer (principals → principal roles → catalog roles → grants) + credential vending, implemented on Iceberg's own `PolarisCatalog` (which itself wraps the same table-metadata machinery from Phase 2). It's an implementation choice, not a different table format.
- 📖 Polaris docs: https://polaris.apache.org/, especially "Getting Started" and "Access Control". Skim the Polaris source (https://github.com/apache/polaris) for `PolarisEntity`/`Grant` model and how it maps role grants to the REST catalog's auth checks.

---

## Phase 6 — REST Fixture vs. Polaris: Compare Head-to-Head

**Goal:** be able to justify a catalog choice in a design review.

- 🛠 Run the identical script (create table, grant/deny, vend credentials, concurrent write from Spark/Trino/Dremio) against both `rest` and `polaris` services side by side and produce a short comparison note covering: multi-tenancy/RBAC, credential vending, multi-catalog federation, external catalog federation (Polaris can front Glue/other catalogs), operational maturity, HA/storage backend for its own metastore.
- 🔬 Both speak the same wire protocol (Phase 4 spec) — the difference is entirely in what sits behind that protocol: the fixture is an in-memory/JDBC reference implementation meant for testing; Polaris is a production-grade multi-tenant service with its own persistence layer for grants/principals.
- 📖 Iceberg REST fixture source (small, worth skimming in full): https://github.com/apache/iceberg/tree/main/open-api — vs. Polaris's `polaris-service` module.

---

## Phase 7 — Table Maintenance & Production Operations

**Goal:** operate a table over time, not just create one.

- 🛠
  - `CALL system.expire_snapshots('db.orders', older_than => ...)` — then re-inspect `metadata.json` and confirm old manifest lists/data files are gone (check MinIO directly).
  - `CALL system.remove_orphan_files(...)` after manually dropping an orphaned file into the data directory.
  - `CALL system.rewrite_data_files('db.orders')` (compaction) after producing many small files (small batched inserts) — measure file count/size before and after.
  - `CALL system.rewrite_manifests('db.orders')` — observe manifest count/size change.
  - Set up a scheduled maintenance job (a simple cron-triggered Spark script) for the above three.
- 🔬 Understand why each exists: unbounded metadata growth, small-file problems from streaming/micro-batch writes, and manifest bloat from many commits — all are *metadata-scale* problems distinct from data-scale problems.
- 📖 Iceberg docs "Maintenance": https://iceberg.apache.org/docs/latest/maintenance/

---

## Phase 8 — Schema & Partition Evolution Deep Dive

**Goal:** understand why Iceberg's evolution is safe (unlike Hive's).

- 🛠
  - Rename a column, widen an int→long, add a nested struct field — verify old snapshots still query correctly (schema-per-snapshot).
  - Partition evolution: change `days(order_ts)` → `hours(order_ts)` on an existing table *without* rewriting old data; write new data; query across the boundary and inspect the query plan to see it uses **partition spec IDs** per manifest to handle both layouts in one scan.
- 🔬 Column IDs (not names) are the actual identity in the file format — renames are metadata-only. Partition specs are versioned per-table, and each manifest records which spec ID it was written under.
- 📖 Spec sections "Schemas and Data Types" (field IDs) and "Partition Evolution".

---

## Phase 9 — Row-Level Changes: Merge-on-Read vs Copy-on-Write

**Goal:** understand delete files, the other half of the format most tutorials skip.

- 🛠
  - Set `write.delete.mode=merge-on-read` vs `copy-on-write` at the table level; run `UPDATE`/`DELETE`/`MERGE INTO` under each; diff the resulting files in MinIO — copy-on-write rewrites data files, merge-on-read writes small **delete files** (positional or equality) alongside.
  - Dump a positional delete file and match its `file_path`/`pos` columns to the spec.
- 🔬 Trade-off: MoR = cheap writes, more expensive reads (must apply deletes at scan time) until compaction; CoW = expensive writes, cheap reads.
- 📖 Spec "Row-level Delete Files" section.

---

## Phase 10 — Capstone: Small Multi-Engine Pipeline on Polaris

**Goal:** put it all together as one small but "real" project.

- 🛠 Build an ingestion path: Spark batch-writes raw events into an Iceberg table cataloged in Polaris → scheduled compaction/expire job → Trino used for ad-hoc analytics queries and Dremio used as a BI-facing semantic layer (virtual datasets + a reflection for a dashboard-style query), both against the same table, under a read-only Polaris principal role. Add basic partitioning/sort strategy justified from Phase 8/9 knowledge, and a runbook covering the Phase 7 maintenance jobs.
- 🔬 By this point you should be able to explain, unprompted, every hop from a Trino or Dremio `SELECT` down to the Parquet bytes in MinIO, and every hop from a Spark `INSERT` up through manifest/metadata commit at Polaris — including where Dremio's reflections sit outside that path entirely (they're a cached/accelerated physical layout Dremio manages itself, not an Iceberg spec concept).
- 📖 Revisit any spec section that still feels fuzzy — this phase is the test.

---

## Suggested pacing

| Phase | Focus | Rough time |
|---|---|---|
| 0 | Environment | 0.5 day |
| 1 | Core SQL ops | 1 day |
| 2 | Metadata internals | 2 days |
| 3 | Trino & Dremio interop | 1.5 days |
| 4 | REST protocol | 1 day |
| 5 | Polaris | 2–3 days |
| 6 | REST vs Polaris | 0.5 day |
| 7 | Maintenance | 1 day |
| 8 | Schema/partition evolution | 1 day |
| 9 | MoR vs CoW | 1 day |
| 10 | Capstone | 2+ days |

## References

- Iceberg spec: https://iceberg.apache.org/spec/
- Iceberg docs: https://iceberg.apache.org/docs/latest/
- Iceberg source: https://github.com/apache/iceberg
- REST Catalog OpenAPI: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
- Polaris docs: https://polaris.apache.org/
- Polaris source: https://github.com/apache/polaris
- Trino Iceberg connector: https://trino.io/docs/current/connector/iceberg.html
- Dremio Iceberg tables: https://docs.dremio.com/current/reference/sql/commands/apache-iceberg-tables/
- Dremio reflections: https://docs.dremio.com/current/sonar/reflections/
