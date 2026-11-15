# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A university e-commerce data pipeline that crawls Tiki.vn product pages and reviews:

```
Selenium crawlers ──► Kafka ──► Spark (Structured Streaming / batch) ──► ClickHouse (raw → bronze → silver)
        ▲                                                                       
        └── PostgreSQL `crawl_state` (URL frontier + scheduling)
```

Comments, notes and many identifiers are in Vietnamese. There is no build system, no linter config and no test suite. Notebooks (`*.ipynb`) are used for ad-hoc checks and data-quality reports.

## Infrastructure (Docker)

- `docker-compose.yml`: the single-node dev stack that is actually used. It runs Postgres (`pg-crawler-db:5432`), one KRaft Kafka broker (`kafka-1:9092`), Spark standalone (`spark-master:7077`, 2 workers, a Spark Connect server on `15002`) and ClickHouse (`clickhouse-server:8123`, database `ecommercial_warehouse`).
- `docker-compose-full.yml` (untracked) is the multi-node variant: 3 Kafka brokers, a ClickHouse keeper plus 2 servers, and a Spark history server. It mounts `docker_mount_full/`.
- Each service sits on its own bridge network (`postgresql-net`, `kafka-net`, `spark-net`, `clickhouse-net`). Spark containers join kafka and clickhouse networks. Use container hostnames (`kafka-1`, `clickhouse-server`, `pg-crawler-db`), not `localhost`, in job code.
- Kafka has `AUTO_CREATE_TOPICS_ENABLE=false`, so topics (`product_data_changes`, `user_review`) must be created manually.
- `dockerfile.spark` builds `spark-custom:4.2.0-scala2.13-java21-python3-ubuntu`, which is the base Spark image plus `python-dotenv`.
- Credentials come from `docker_secrets/*.txt` and `.env` (copy `.env.template`). The Spark jobs currently hardcode ClickHouse credentials.
- Disk usage: anything a container writes outside a bind mount lands in `E:\DockerDesktopWSL\disk\docker_data.vhdx`, and that file never shrinks. Shrinking it takes a manual compaction (`docs/disk_usage/compact_vhdx.ps1`, with a UAC prompt).
  - Spark workers therefore set `SPARK_LOCAL_DIRS=/opt/spark/local`, bind-mounted to `docker_mount/spark_standalone/spark_worker_N/local`, and enable `spark.worker.cleanup` (removes `work/app-*` dirs older than 3 days).
  - ClickHouse loads `docker_mount/clickhouse/config.d/zz-low-disk.xml`, mounted as a single file so it doesn't shadow the image's `docker_related_config.xml`. It sets log level `warning`, turns off trace/text/metric system logs, gives `query_log` and `part_log` a 7-day TTL, and caps spill at 10 GB.
  - Details are in `docs/DISK_USAGE_REPORT.md`.

```bash
docker compose up -d --build          # main stack
cd src/python/crawler && docker compose -f crawler-docker-compose.yml up -d --build   # crawlers (each needs <name>.env from its .env.template)
```

## Running Spark jobs

Job scripts are **not** volume-mounted into the Spark containers. The user copies them into `spark_master` under `/opt/spark/work-dir/spark_job/` themselves. The local and container layouts differ: local `src/python/ETL/<layer>/` corresponds to `/opt/spark/work-dir/spark_job/ETL/<layer>/` in the container. Do not `docker cp` code into containers. After editing a job, tell the user which files changed. A stale copy in the container is a common reason a change "has no effect".

Run jobs from inside `spark_master`, from the job's own directory, because imports are flat and relative to the script location (for example `from preprocessing_func.product_snapshot_norm import *` and `from spark_streaming_listener import ...`). This is the command from `COMMAND_NOTE.txt`:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --deploy-mode client \
  --conf spark.driver.host=spark-master --conf spark.driver.bindAddress=0.0.0.0 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,com.clickhouse:clickhouse-jdbc:0.8.3 \
  --conf spark.jars.ivy=/tmp/.ivy \
  load_bronze_product_snapshot.py
```

Resources are tight: every container is limited to 1 core and 1g, except ClickHouse with 2 cores and 3g. Keep driver and executor memory around 500m.

## Architecture

### Crawlers (`src/python/crawler/`)
- `base_crawler.py` defines the `BaseCrawler` ABC. It handles Selenium Edge driver lifecycle, profile dirs, orphan-process cleanup and signal handling, and it is used as a context manager. Subclasses implement `run()`.
- `discovery_crawler.py` starts from `SEED_URL`, finds product URLs and inserts them into Postgres `crawl_state`. It does not touch Kafka.
- `refresh_crawler.py` and `comment_crawler.py` claim due URLs with `src/sql_script/postgresql/query/get_sheduled_url.sql`. That query uses `UPDATE ... FOR UPDATE SKIP LOCKED` to bump `next_crawled_at`, which lets several workers share the queue. They scrape the page and produce JSON to Kafka (`product_data_changes` and `user_review`), keyed by `url_id`. They also write per-URL timing metrics CSVs (`observed_at`, `produced_at`, kafka partition and offset).
- Every Kafka message carries `observed_at`, the time the crawler saw the page. Downstream time semantics depend on it, not on the Kafka timestamp.
- Each crawler has its own Dockerfile (python:3.11-slim plus Edge and msedgedriver) and its own `.env.template`.

### Spark ETL (`src/python/ETL/`), with layers ordered by folder prefix
- `a_raw/`: Structured Streaming from Kafka into ClickHouse `raw_*` tables through `foreachBatch` and JDBC append. Every field stays a trimmed string. `idempotency_key = sha2(url_id ‖ observed_at)` and the job deduplicates on it. Checkpoints go to `/opt/spark/checkpoints/<job>_ck`, which is mounted. `spark_streaming_listener.CSVStreamingQueryListener` writes progress metrics to `/opt/spark/metrics/`.
- `b_bronze/`: batch JDBC read of `raw_*`, then type casting and normalization through Column-expression helpers in `preprocessing_func/*_norm.py` (price, discount, sold, title, brand, warranty, rating), then append to `bronze_*`. `brand` keeps the original name and `brand_normalized` supports entity resolution for brand and seller. The product job dedups by `idempotency_key` inside its ClickHouse read query (`RAW_DEDUP_QUERY`). It finds duplicate keys through `cityHash64`, passes unique rows through, and runs `GROUP BY` only on the duplicates. Do not reintroduce Spark `dropDuplicates` on a near-unique key: with 500m executors it spilled about 16.6 GB per run.
- `c_silver/`: `.sql` files plus a test notebook for silver dimensions and facts (`silver_product`, `silver_brand`, `silver_seller`, `silver_category`, `silver_snapshot`). This layer is in progress. Silver SKs are hashes of attributes, so each one is a version key. `url_id` is the durable product key.
- `d_gold/`: a ClickHouse star schema with DDL in `src/sql_script/clickhouse/ddl/gold_ddl.sql`. Loaders run in file-number order: `01`–`06` build the dims, `07` builds `gold_fact_product_snapshot`, and `08`–`11` build facts derived from `07`. Every table is `ReplacingMergeTree(updated_at)` and the loaders recompute everything, so reruns are idempotent. Query with `FINAL`. Gold re-keys brand by `brand_normalized` only and seller by name only. Silver includes origin and rating in those keys. Reviewer and review tables read `bronze_user_review` because silver review tables don't exist yet. Bronze maps blank "sold" and rating-count cells to 0, so F1 has spurious zeros. Loader `08` (F2) takes a running max of `sold_cum` and `product_rate_cnt` per product before computing daily deltas. Never use the raw last value of a cumulative counter (GOLD_LAYER.md 13.16). `GOLD_LAYER.md` at the repo root is the gold data dictionary: grain, columns, relationships, lineage, and the images in `docs/images/`. Update it whenever you change the gold DDL or loaders.
- `e_mart/`: dashboard-facing data marts (`mart_*`) built from gold, with DDL and the shared UDFs `mart_price_band` / `mart_discount_band` in `src/sql_script/clickhouse/ddl/mart_ddl.sql`. Loaders `01`–`08` run in order, and `01` (`mart_kpi_daily`, which owns `is_reliable_day`) must run first. Each loader builds `<mart>__stage` and then runs `ALTER TABLE <mart> REPLACE PARTITION tuple() FROM <mart>__stage`. That makes reloads atomic and idempotent, so marts are plain `MergeTree` without `PARTITION BY` and are queried without `FINAL`. `EXCHANGE TABLES` fails on this Windows bind mount (`renameat2` unsupported). Marts store additive components (sums and counts), and ratios are computed at query time. The dashboard SQL (6 files, 43 queries, ClickHouse `{param:Type}` syntax) lives in `src/sql_script/clickhouse/query_dashboard/`. `MART_LAYER.md` is the mart data dictionary, with per-column tables and the lineage diagram `docs/images/mart_lineage.svg`. `query_dashboard/README.md` is the chart catalog. Update all three when you change marts or dashboard queries.
- `src/python/dashboard/`: the Streamlit app for those 43 queries. It runs as compose service `dashboard` (container `streamlit_dashboard`, port 8501, on `clickhouse-net`). `core/sql_catalog.py` parses the `.sql` files on `-- [x.y] Title | Biểu đồ:` headers, and the SQL runs verbatim with server-side params, so no SQL is copied into Python. If you rename an output column, update the matching `views/<n>_*.py`. Code and SQL are bind-mounted read-only: SQL and `views/` edits apply on rerun, while `core/` and `.streamlit/` edits need `docker restart streamlit_dashboard`. Start it only with `docker compose up -d --no-deps [--build] dashboard`, because without `--no-deps` compose recreates `clickhouse_server`. Check it with `MSYS_NO_PATHCONV=1 docker exec -w /app streamlit_dashboard python smoke_test.py`. Its README is `src/python/dashboard/README.md`.
- `src/sql_script/clickhouse/test_data/` holds a deterministic synthetic dataset. It generates about 10M `raw_product_snapshot` rows (50k products × 200 crawls) and about 525k `raw_user_review` rows, all with `url_id >= 1000000`, directly in ClickHouse SQL. `bash src/sql_script/clickhouse/test_data/run_test_pipeline.sh all|generate|bronze|silver|gold|mart|check|cleanup` runs the whole pipeline. It never truncates raw, but it truncates and rebuilds bronze, silver and gold, because the bronze job and the silver snapshot loader append everything they read. `mart` needs no truncate, and `check` also runs `check_mart.sql` when the marts exist.
- You can test SQL without touching server data: pipe a script to `docker exec -i clickhouse_server clickhouse local --multiquery`, and pull rows in with `remote('localhost:9000', 'ecommercial_warehouse', '<table>', 'clickhouse', '<pw>')`.
- `src/python/spark_job/spark_kafka_to_clickhouse*.py` are older single-step jobs from before the layered design, kept for evaluation evidence.

### SQL (`src/sql_script/`)
- `clickhouse/ddl/{raw,bronze,silver}_ddl.sql` holds the MergeTree tables. Raw and bronze use `PARTITION BY toDate(ingested_at) ORDER BY (url_id, observed_at)`. If you change a Spark job's output columns, update the matching DDL too, and the reverse.
- `clickhouse/data_quality/` holds the quality-report queries, with notebooks in `src/python/spark_job/data_quality_check/`. `bash src/sql_script/clickhouse/data_quality/run_dq.sh all|raw|bronze|silver|gold|cross|report` runs the layered DQ suite (`01`–`05`, about 276 checks, about 7 minutes on the 10M test set). It writes one row per check to `dq_check_result` (PASS/WARN/FAIL/INFO, run history) and exports a report (`.txt` and `.md`, from the same `99_dq_report.sql` via `--format`) plus CSV to `LOG_DIR`. Shared `dq_*` SQL UDFs live in `00_dq_setup.sql`. It only reads pipeline tables. `raw_data_quality.sql` is the older standalone report. `data_quality/README.md` is the user guide and check catalog (every check ID with its thresholds); update it when you add or change checks.
- `postgresql/ddl/crawl_state_ddl.sql` holds the crawl frontier schema (status, adaptive `crawl_interval`, error counters).

### Evidence (`evidence/wNN/`)
These are weekly deliverables for the course: metrics CSVs, eval scripts, figures and a `manifest.json`. `TEACHER_NOTE.md` lists the instructor's open requirements. The active ones are brand and seller entity resolution and a robust idempotency and duplicate story when Spark re-reads Kafka.
