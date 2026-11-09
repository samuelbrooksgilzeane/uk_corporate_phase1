"""
Generic Companies House streaming consumer (one stream per job).

Runs in Databricks, appends immutable event envelopes to Bronze, and only
advances its Delta checkpoint after the corresponding append commits.

WARNING: A live start is not a historical backfill. Do not merge this stream
with a bulk snapshot until the snapshot-to-stream handoff is reconciled.

  python ingestion/capture_ch_stream.py --stream psc --live-start
  python ingestion/capture_ch_stream.py --stream psc  # resume
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import time

STREAM_PATHS = {
    "companies": "companies",
    "filings": "filings",
    "insolvency": "insolvency-cases",
    "charges": "charges",
    "officers": "officers",
    "psc": "persons-with-significant-control",
    "disqualifications": "disqualified-officers",
    "exemptions": "company-exemptions",
    "psc_statements": "persons-with-significant-control-statements",
}
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
ROOT = "https://stream.companieshouse.gov.uk"


def stream_api_key(spark):
    key = os.getenv("COMPANIES_HOUSE_STREAM_KEY")
    if key:
        return key
    scope = os.getenv("CH_STREAM_SECRET_SCOPE")
    if scope:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark).secrets.get(
            scope, os.getenv("CH_STREAM_SECRET_KEY", "stream_key")
        )
    raise RuntimeError("Configure a dedicated Companies House streaming key")


def run(stream, catalog, schema, start_timepoint, live_start, max_minutes, batch_size):
    import requests
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        LongType, StringType, StructField, StructType, TimestampType
    )

    for identifier in (catalog, schema):
        if not IDENTIFIER.fullmatch(identifier):
            raise ValueError("Invalid Unity Catalog identifier")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    prefix = f"`{catalog}`.`{schema}`"
    source_table = f"{prefix}.`ch_stream_events`"
    checkpoint_table = f"{prefix}.`ch_stream_checkpoints`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {source_table} (
        stream_name STRING, event_timepoint BIGINT,
        resource_uri STRING, resource_id STRING, resource_kind STRING,
        event_type STRING, published_at STRING, data_json STRING,
        raw_json STRING, ingested_at TIMESTAMP
      ) USING DELTA
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {checkpoint_table} (
        stream_name STRING, last_timepoint BIGINT, updated_at TIMESTAMP
      ) USING DELTA
    """)
    row = spark.sql(
        f"SELECT last_timepoint FROM {checkpoint_table} WHERE stream_name = '{stream}'"
    ).first()
    checkpoint = row["last_timepoint"] if row else start_timepoint
    if checkpoint is None and not live_start:
        raise RuntimeError(
            "No checkpoint: specify --live-start (events from now) or a verified "
            "--start-timepoint. Never assume a bulk ZIP contains this offset."
        )

    row_schema = StructType([
        StructField("stream_name", StringType(), False),
        StructField("event_timepoint", LongType(), False),
        StructField("resource_uri", StringType(), True),
        StructField("resource_id", StringType(), True),
        StructField("resource_kind", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("data_json", StringType(), True),
        StructField("raw_json", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
    ])
    buffer: list[tuple] = []
    newest_seen = checkpoint
    written = 0
    deadline = time.monotonic() + max_minutes * 60
    session = requests.Session()
    session.auth = (stream_api_key(spark), "")

    def flush():
        nonlocal written
        if newest_seen is None:
            return
        if buffer:
            spark.createDataFrame(buffer, schema=row_schema).write.format(
                "delta"
            ).mode("append").saveAsTable(source_table)
            written += len(buffer)
            buffer.clear()
        # Checkpoint must be committed *after* the batch.
        spark.sql(f"""
          MERGE INTO {checkpoint_table} t
          USING (SELECT '{stream}' AS stream_name,
                        {int(newest_seen)}L AS last_timepoint) s
          ON t.stream_name=s.stream_name
          WHEN MATCHED AND s.last_timepoint > t.last_timepoint
            THEN UPDATE SET last_timepoint=s.last_timepoint,
                            updated_at=current_timestamp()
          WHEN NOT MATCHED THEN INSERT (stream_name,last_timepoint,updated_at)
            VALUES (s.stream_name,s.last_timepoint,current_timestamp())
        """)

    while time.monotonic() < deadline:
        params = {"timepoint": int(newest_seen)} if newest_seen is not None else {}
        try:
            with session.get(
                f"{ROOT}/{STREAM_PATHS[stream]}", params=params,
                stream=True, timeout=(15, 120)
            ) as response:
                if response.status_code == 416:
                    raise RuntimeError("Stream offset expired; rebootstrap required")
                if response.status_code == 429:
                    time.sleep(60)
                    continue
                if response.status_code >= 500:
                    time.sleep(10)
                    continue
                response.raise_for_status()
                for line in response.iter_lines():
                    if time.monotonic() >= deadline:
                        break
                    if not line:  # heartbeat
                        continue
                    document = json.loads(line)
                    meta = document.get("event") or {}
                    if meta.get("timepoint") is None:
                        continue
                    timepoint = int(meta["timepoint"])
                    if newest_seen is not None and timepoint <= newest_seen:
                        continue
                    newest_seen = timepoint
                    now = datetime.now(timezone.utc).replace(tzinfo=None)
                    buffer.append((
                        stream, timepoint, document.get("resource_uri"),
                        document.get("resource_id"), document.get("resource_kind"),
                        meta.get("type"), meta.get("published_at"),
                        json.dumps(document.get("data"), separators=(",", ":"), sort_keys=True),
                        json.dumps(document, separators=(",", ":"), sort_keys=True),
                        now,
                    ))
                    if len(buffer) >= batch_size:
                        flush()
        except (requests.Timeout, requests.ConnectionError):
            time.sleep(10)
        finally:
            flush()
    flush()
    print(f"{stream}: wrote {written} raw events; checkpoint={newest_seen}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", required=True, choices=sorted(STREAM_PATHS))
    parser.add_argument("--catalog", default="uk_corporate")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--start-timepoint", type=int)
    parser.add_argument("--live-start", action="store_true")
    parser.add_argument("--max-minutes", type=int, default=60)
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()
    if args.max_minutes < 1 or args.batch_size < 1:
        parser.error("max-minutes and batch-size must be positive")
    run(
        args.stream, args.catalog, args.schema,
        args.start_timepoint, args.live_start, args.max_minutes, args.batch_size
    )


if __name__ == "__main__":
    main()
