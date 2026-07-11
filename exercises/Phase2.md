### 0. Make sure you have a live table to inspect

MinIO has no persistent volume in this stack, so every `docker compose down`/`up` wipes the `warehouse` bucket. If you get empty results below, redo Phase 1 first (or just re-run this):

docker exec -it spark-iceberg spark-sql

CREATE NAMESPACE IF NOT EXISTS exercise;

CREATE TABLE IF NOT EXISTS exercise.orders (
  order_id BIGINT,
  customer STRING,
  amount DECIMAL(10,2),
  order_ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(order_ts));

INSERT INTO exercise.orders VALUES (1, 'alice', 100.00, TIMESTAMP '2024-01-01 10:00:00');
INSERT INTO exercise.orders VALUES (2, 'bob',   250.50, TIMESTAMP '2024-01-02 11:30:00');
INSERT INTO exercise.orders VALUES (3, 'carol',  75.25, TIMESTAMP '2024-01-03 09:15:00');

Exit spark-sql (Ctrl-D) once this is done — the rest of this phase happens from the host shell against the mc/spark-iceberg containers.

### 1. Browse the raw files MinIO is holding

No `jq` is installed in these containers, so we'll lean on `mc`, `python3`, `strings`, and `xxd`, which are all present.

docker exec mc mc ls -r minio/warehouse

You should see three parallel groups under `exercise/orders/`:
- `data/order_ts_day=.../*.parquet` — the actual rows, one file per day partition.
- `metadata/*.metadata.json` — one per DDL/DML, growing (00000, 00001, 00002, 00003...).
- `metadata/*.avro` — two kinds mixed together: files named `snap-<snapshot_id>-...avro` (manifest **lists**) and files named `<uuid>-m0.avro` (manifest **files**).

Learn: this one flat `metadata/` folder holds the entire commit history — every old `metadata.json`, every manifest list, every manifest file ever written. Nothing is overwritten in place; new commits only add files and swap which one is "current".

### 2. Pull down the latest metadata.json and read it

Find your highest-numbered `metadata.json` from step 1's output, then:

docker exec spark-iceberg bash -lc 'aws s3 --endpoint-url http://minio:9000 cp s3://warehouse/exercise/orders/metadata/<your-latest>.metadata.json /tmp/latest.metadata.json'

docker exec spark-iceberg python3 -c "
import json
d = json.load(open('/tmp/latest.metadata.json'))
print('format-version:', d['format-version'])
print('current-snapshot-id:', d['current-snapshot-id'])
print('num snapshots:', len(d['snapshots']))
print('num schemas:', len(d['schemas']))
print('num partition-specs:', len(d['partition-specs']))
print()
print('snapshot log (oldest -> newest):')
for s in d['snapshots']:
    print(' ', s['snapshot-id'], s['timestamp-ms'], s.get('manifest-list'))
"

Learn: `current-snapshot-id` is the ONE pointer that defines "what this table looks like right now." Every snapshot entry has its own `manifest-list` path — that's your link to step 3. Note `format-version: 2` — this is what enables row-level deletes later (Phase 9).

### 3. Open a manifest list — the "table of contents" for one snapshot

Pick the `manifest-list` path for your current (latest) snapshot from step 2's output, then pull it down:

docker exec spark-iceberg bash -lc 'aws s3 --endpoint-url http://minio:9000 cp s3://warehouse/exercise/orders/metadata/snap-<snapshot_id>-1-<uuid>.avro /tmp/manifest-list.avro'

First, prove to yourself it's really Avro, and see its embedded schema (Avro Object Container Files store their writer schema as JSON right after the `Obj\x01` magic bytes):

docker exec spark-iceberg bash -lc "xxd /tmp/manifest-list.avro | head -3"
docker exec spark-iceberg bash -lc "strings /tmp/manifest-list.avro | grep -o '{\"type\":\"record\".*}' | head -c 600"

You should see `"name":"manifest_file"` with fields like `manifest_path`, `manifest_length`, `added_data_files_count`, `existing_data_files_count`, `deleted_data_files_count` — that's the manifest list schema straight from the spec.

Now see the same data actually decoded, via Iceberg's own SQL metadata table (this is Iceberg parsing that exact Avro file for you):

docker exec spark-iceberg spark-sql -e "SELECT path, added_snapshot_id, added_data_files_count, existing_data_files_count, deleted_data_files_count FROM exercise.orders.manifests;"

Learn: a manifest **list** doesn't contain data-file info directly — it lists *manifest files*, one row per manifest, with per-manifest counts. This is the layer that lets a query engine skip whole manifests without opening them, if none of their files could match a filter.

### 4. Open a manifest file — the actual per-data-file stats

Pick one `manifest_path` from step 3's `.manifests` output (a `<uuid>-m0.avro` file) and pull it down:

docker exec spark-iceberg bash -lc 'aws s3 --endpoint-url http://minio:9000 cp s3://warehouse/exercise/orders/metadata/<uuid>-m0.avro /tmp/manifest.avro'

docker exec spark-iceberg bash -lc "strings /tmp/manifest.avro | grep -o '{\"type\":\"record\".*}' | head -c 400"

You should see `"name":"manifest_entry"` wrapping a nested `"name":"r2"` record — that nested record is `data_file`, with fields `file_path`, `file_format`, `partition`, `record_count`, `column_sizes`, `value_counts`, `null_value_counts`, `lower_bounds`, `upper_bounds`.

Now decode it via SQL, and actually look at the min/max stats that power file pruning:

docker exec spark-iceberg spark-sql -e "SELECT file_path, partition, record_count, lower_bounds, upper_bounds FROM exercise.orders.files;"

docker exec spark-iceberg spark-sql -e "SELECT status, snapshot_id, sequence_number FROM exercise.orders.entries;"

Learn: `lower_bounds`/`upper_bounds` are per-column min/max values Iceberg tracked *while writing*, at no read-time cost. A query with `WHERE order_ts = '2024-01-05'` can skip a data file entirely just by checking these bounds in the manifest — before ever opening the Parquet file. Also note `entries.status`: `1` means ADDED in that snapshot — this is how Iceberg tells "new in this commit" from "carried over from a previous manifest" without rewriting anything.

### 5. Trace the full chain by hand, once, top to bottom

Using only the outputs you already collected in steps 2-4, write down on paper (or in a scratch file) the actual chain for ONE data file, following every ID:

1. `metadata.json` → `current-snapshot-id` = `<snapshot_id>`
2. that snapshot's `manifest-list` = `snap-<snapshot_id>-...avro`
3. one row in that manifest list → `manifest_path` = `<uuid>-m0.avro`, with `added_data_files_count = 1`
4. one row in that manifest file → `data_file.file_path` = `.../data/order_ts_day=2024-01-0X/....parquet`, plus its `record_count` and `lower_bounds`/`upper_bounds`
5. confirm that Parquet file actually exists at that path:

docker exec mc mc stat minio/warehouse/exercise/orders/data/order_ts_day=2024-01-01/<file>.parquet

Learn: you just manually replayed exactly what Spark's/Trino's query planner does on every scan — metadata.json → manifest list → manifest → data file — except the engine does it in milliseconds and prunes branches using the stats you saw in step 4.

### 6. See metadata grow, and predict why it needs maintenance later

Run one more insert, then re-check:

docker exec spark-iceberg spark-sql -e "INSERT INTO exercise.orders VALUES (4, 'dave', 10.00, TIMESTAMP '2024-01-04 08:00:00');"

docker exec mc mc ls -r minio/warehouse/exercise/orders/metadata | wc -l

docker exec spark-iceberg spark-sql -e "SELECT count(*) FROM exercise.orders.snapshots;"

Learn: every commit adds one more `metadata.json`, one more manifest list, and at least one more manifest file — none of the old ones are deleted. This is exactly the unbounded growth that Phase 7's `expire_snapshots` exists to clean up. You're now seeing the *reason* for that maintenance operation, not just being told to run it.

## What you should walk away understanding

* A table's on-disk footprint is three distinct layers — `metadata.json` (table state + schema/partition history), manifest lists (per-snapshot manifest index), manifest files (per-file stats) — sitting on top of the actual data files, and each layer exists to let engines avoid reading the layer below it.
* Iceberg's SQL metadata tables (`.snapshots`, `.manifests`, `.entries`, `.files`, `.history`) aren't a separate feature — they're a decoded view of the exact Avro/JSON files you opened by hand in steps 2-4.
* Min/max column bounds live in the manifest, not the data file header — that's what lets an engine prune files without touching Parquet footers.
* Nothing is ever mutated in place; every operation appends new metadata/manifest/data files and swaps one pointer. That's *why* time travel, rollback, and multi-engine concurrency (Phase 3) all work safely.
* This append-only model is also why metadata keeps growing forever until you explicitly run maintenance (Phase 7) — you just watched it happen in step 6.
