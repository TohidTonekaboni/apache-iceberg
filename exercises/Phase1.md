### 1. Create a namespace and an Iceberg table, partitioned by day

Either use the Jupyter notebook at localhost:8888, or — to sidestep the Jupyter websocket flakiness from earlier — exec straight into the container:

docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}"

docker exec -it spark-iceberg spark-sql


CREATE NAMESPACE IF NOT EXISTS exercise;

CREATE TABLE exercise.orders (
  order_id BIGINT,
  customer STRING,
  amount DECIMAL(10,2),
  order_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(order_ts));

Learn: this is a metadata-only operation — no data files exist yet, just a fresh metadata.json in s3://warehouse/wh/db/orders/metadata/.

### 2. Insert data in three separate batches (so you get three distinct snapshots)

INSERT INTO exercise.orders VALUES (1, 'alice', 100.00, TIMESTAMP '2024-01-01 10:00:00');
INSERT INTO exercise.orders VALUES (2, 'bob',   250.50, TIMESTAMP '2024-01-02 11:30:00');
INSERT INTO exercise.orders VALUES (3, 'carol',  75.25, TIMESTAMP '2024-01-03 09:15:00');

SELECT * FROM exercise.orders ORDER BY order_id;

Learn: each INSERT is its own atomic commit — a new snapshot, a new metadata.json version, one compare-and-swap at the catalog.

### 3. Inspect snapshot history

SELECT snapshot_id, committed_at, operation FROM exercise.orders.snapshots ORDER BY committed_at;

SELECT * FROM exercise.orders.history;

Learn: Note the committed_at timestamps and 3 distinct snapshot_ids — one per insert. Keep the snapshot IDs handy for step 5.

### 4. Evolve the schema and partitioning without touching existing data

ALTER TABLE exercise.orders ADD COLUMN region STRING;

ALTER TABLE exercise.orders ADD PARTITION FIELD hours(order_ts);

SELECT * FROM exercise.orders ORDER BY order_id;

Learn: the old data files are untouched — old partition spec still applies to them, new writes will use the new spec. Two partition specs now coexist for one table.

### 5. Time travel

-- pick a snapshot_id from step 3
SELECT * FROM exercise.orders VERSION AS OF <snapshot_id>;

SELECT * FROM exercise.orders TIMESTAMP AS OF '2024-01-02 12:00:00';

Learn: this just swaps which metadata.json snapshot entry the read plan resolves against — no data is copied or moved.

### 6. Rollback

CALL demo.system.rollback_to_snapshot('db.orders', <first_snapshot_id>);

SELECT * FROM exercise.orders ORDER BY order_id;
Learn: rollback is also just a metadata pointer change — the newer snapshots aren't deleted, they're just no longer "current" (they still exist until expire_snapshots in Phase 7).

## What you should walk away understanding

* A table = a pointer to a metadata.json, not a folder of files.

* Every DDL/DML is a new metadata version + one atomic catalog commit.

* Schema and partition evolution are metadata-only — no data rewrite.

* Time travel / rollback = pointing reads at a different (still-existing) snapshot.