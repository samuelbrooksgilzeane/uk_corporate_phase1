"""
Optional stream *capture* job, separate from the dbt source table.

It records Companies House stream events and an independently resumable
checkpoint. DO NOT automatically merge captured events into psc_events:
first establish/validate their cutover against the baseline.

Example:
  python capture_psc_stream.py --companies 01234567,SC123456 --live-start
  python capture_psc_stream.py --companies 01234567,SC123456
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import re
import time

import requests

from ingest_psc_cohort import parse_companies, IDENTIFIER_RE


STREAM_URL = "https://stream.companieshouse.gov.uk/persons-with-significant-control"
COMPANY_FROM_URI = re.compile(r"^/company/([^/]+)/")


def resolve_stream_key(spark):
    import os

    key = os.getenv("COMPANIES_HOUSE_STREAM_KEY")
    if key:
        return key
    scope = os.getenv("CH_STREAM_SECRET_SCOPE")
    if scope:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark).secrets.get(
            scope, os.getenv("CH_STREAM_SECRET_KEY", "stream_key")
        )
    raise RuntimeError("Set a separate Companies House streaming key")


def run(
    companies: list[str], catalog: str, schema: str,
    live_start: bool, start_timepoint: int | None,
    max_minutes: int, batch_size: int,
) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        LongType, StringType, StructField, StructType, TimestampType
    )

    for name in (catalog, schema):
        if not IDENTIFIER_RE.fullmatch(name):
            raise ValueError("Invalid catalog/schema identifier")
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    prefix = f"`{catalog}`.`{schema}`"
    stream_table = f"{prefix}.`psc_stream_capture`"
    checkpoint_table = f"{prefix}.`psc_stream_checkpoints`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {prefix}")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {stream_table} (
        source_timepoint BIGINT, company_number STRING, resource_uri STRING,
        event_type STRING, published_at STRING, raw_json STRING,
        received_at TIMESTAMP
      ) USING DELTA
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {checkpoint_table} (
        stream_name STRING, last_timepoint BIGINT, updated_at TIMESTAMP
      ) USING DELTA
    """)

    scope = "psc_" + hashlib.sha256(",".join(sorted(companies)).encode()).hexdigest()[:16]
    existing = spark.sql(
        f"SELECT last_timepoint FROM {checkpoint_table} WHERE stream_name = '{scope}'"
    ).first()
    offset = existing["last_timepoint"] if existing else start_timepoint
    if offset is None and not live_start:
        raise RuntimeError(
            "No checkpoint. First run needs --live-start (starts NOW, not historical) "
            "or --start-timepoint from a validated source."
        )

    schema_type = StructType([
        StructField("source_timepoint", LongType(), False),
        StructField("company_number", StringType(), True),
        StructField("resource_uri", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("raw_json", StringType(), False),
        StructField("received_at", TimestampType(), False),
    ])
    buffer: list[tuple] = []
    newest_seen = offset
    captured_count = 0
    deadline = time.monotonic() + max_minutes * 60
    session = requests.Session()
    session.auth = (resolve_stream_key(spark), "")

    def flush() -> None:
        nonlocal captured_count
        if newest_seen is None:
            return
        if buffer:
            spark.createDataFrame(buffer, schema_type).write.format("delta").mode(
                "append"
            ).saveAsTable(stream_table)
            captured_count += len(buffer)
            buffer.clear()
        # Commit the checkpoint ONLY AFTER all selected rows are durably written.
        spark.sql(f"""
          MERGE INTO {checkpoint_table} t
          USING (SELECT '{scope}' AS stream_name,
                        {int(newest_seen)}L AS last_timepoint) s
          ON t.stream_name = s.stream_name
          WHEN MATCHED AND s.last_timepoint > t.last_timepoint
            THEN UPDATE SET last_timepoint=s.last_timepoint, updated_at=current_timestamp()
          WHEN NOT MATCHED THEN INSERT (stream_name, last_timepoint, updated_at)
            VALUES (s.stream_name, s.last_timepoint, current_timestamp())
        """)

    while time.monotonic() < deadline:
        params = {"timepoint": int(newest_seen)} if newest_seen is not None else {}
        try:
            with session.get(
                STREAM_URL, params=params, stream=True, timeout=(15, 120)
            ) as response:
                if response.status_code == 416:
                    raise RuntimeError("PSC stream offset expired: manual re-bootstrap needed")
                if response.status_code == 429:
                    time.sleep(60)
                    continue
                if response.status_code in (401, 403):
                    response.raise_for_status()
                if response.status_code >= 500:
                    time.sleep(10)
                    continue
                response.raise_for_status()

                for line in response.iter_lines():
                    if time.monotonic() >= deadline:
                        break
                    if not line:  # API heartbeat
                        continue
                    event = json.loads(line)
                    details = event.get("event") or {}
                    timepoint = details.get("timepoint")
                    if timepoint is None:
                        continue
                    timepoint = int(timepoint)
                    if newest_seen is not None and timepoint <= newest_seen:
                        continue  # replayed event after reconnection
                    newest_seen = timepoint
                    uri = event.get("resource_uri") or ""
                    match = COMPANY_FROM_URI.match(uri)
                    company = match.group(1).upper() if match else None
                    if company in companies:
                        buffer.append((
                            timepoint, company, uri, details.get("type"),
                            details.get("published_at"),
                            json.dumps(event, sort_keys=True, separators=(",", ":")),
                            datetime.now(timezone.utc).replace(tzinfo=None)
                        ))
                    if timepoint % batch_size == 0 or len(buffer) >= batch_size:
                        flush()
        except (requests.Timeout, requests.ConnectionError):
            time.sleep(10)
        finally:
            flush()

    flush()
    print(f"Captured {captured_count} scoped PSC events; last stream offset: {newest_seen}")
    return captured_count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--companies", required=True)
    parser.add_argument("--catalog", default="uk_corporate")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--live-start", action="store_true")
    parser.add_argument("--start-timepoint", type=int)
    parser.add_argument("--max-minutes", type=int, default=60)
    parser.add_argument("--batch-size", type=int, default=250)
    args = parser.parse_args()
    if args.max_minutes < 1 or args.batch_size < 1:
        parser.error("--max-minutes and --batch-size must be positive")
    run(
        parse_companies(args.companies), args.catalog, args.schema,
        args.live_start, args.start_timepoint, args.max_minutes, args.batch_size
    )


if __name__ == "__main__":
    main()
