"""
Databricks Python job: poll PSC records for a small, explicit company cohort.

This produces the Bronze event contract consumed by the Phase 1 dbt models.
It records changes observed *between polls*; it cannot recover intra-day states.

Example (Databricks Python file task or notebook job):
  python ingest_psc_cohort.py --companies 01234567,SC123456 --catalog uk_corporate

Configure COMPANIES_HOUSE_API_KEY as a Databricks secret-backed environment
variable, or configure CH_SECRET_SCOPE / CH_SECRET_KEY for DBUtils lookup.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import time
from typing import Any

API_ROOT = "https://api.company-information.service.gov.uk"
COMPANY_RE = re.compile(r"^[A-Za-z0-9]{8}$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAX_COMPANIES = 100
MAX_RECORDS = 10_000


def canonical_json(value: dict[str, Any]) -> str:
    """Stable JSON encoding so formatting changes do not look like updates."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def parse_companies(value: str) -> list[str]:
    companies = list(dict.fromkeys(x.strip().upper() for x in value.split(",") if x.strip()))
    if not companies or len(companies) > MAX_COMPANIES:
        raise ValueError(f"Supply 1–{MAX_COMPANIES} company numbers")
    for number in companies:
        if not COMPANY_RE.fullmatch(number):
            raise ValueError(f"Invalid company number format: {number!r}")
    return companies


def plan_changes(
    fetched: dict[str, tuple[str, str]],
    previous: dict[str, tuple[str, str | None, str]],
) -> list[tuple[str, str, str, str | None]]:
    """Return (psc_uri, company_number, event_type, payload_json) events.

    Previous state contains (company_number, payload_json, event_type).
    The caller must fetch every page for every company before invoking this.
    """
    result: list[tuple[str, str, str, str | None]] = []
    previously_seen_companies = {item[0] for item in previous.values()}

    for uri, (company, payload) in fetched.items():
        old = previous.get(uri)
        if old is None:
            kind = "baseline" if company not in previously_seen_companies else "changed"
            result.append((uri, company, kind, payload))
        elif old[2] == "deleted" or old[1] != payload:
            result.append((uri, company, "changed", payload))

    for uri, (company, _payload, event_type) in previous.items():
        if uri not in fetched and event_type != "deleted":
            result.append((uri, company, "deleted", None))

    return sorted(result, key=lambda record: record[0])


def request_page(
    session: requests.Session, company_number: str, offset: int
) -> dict[str, Any]:
    url = f"{API_ROOT}/company/{company_number}/persons-with-significant-control"
    params = {"start_index": offset, "items_per_page": 100, "register_view": "false"}
    for attempt in range(5):
        response = session.get(url, params=params, timeout=(10, 60))
        if response.status_code == 429:
            # Companies House recommends backing off, rather than retrying rapidly.
            time.sleep(60)
            continue
        if response.status_code in (500, 502, 503, 504):
            time.sleep(min(10 * (attempt + 1), 60))
            continue
        response.raise_for_status()  # Includes 401/403/404: never infer deletions.
        return response.json()
    raise RuntimeError(f"PSC API did not recover for company {company_number}")


def fetch_company_pscs(
    session: requests.Session, company_number: str
) -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    offset = 0
    while True:
        body = request_page(session, company_number, offset)
        items = body.get("items", [])
        for item in items:
            uri = (item.get("links") or {}).get("self")
            if not uri or "/persons-with-significant-control/" not in uri:
                raise ValueError(f"Unexpected PSC identifier for {company_number}")
            result[uri] = (company_number, canonical_json(item))
        offset += len(items)
        total = body.get("total_results")
        if total is not None and offset >= int(total):
            break
        if not items:
            if total is not None and offset < int(total):
                raise ValueError(f"Truncated PSC pages for {company_number}")
            break
        if total is None and len(items) < 100:
            break
    return result


def resolve_api_key(spark: Any) -> str:
    key = os.getenv("COMPANIES_HOUSE_API_KEY")
    if key:
        return key
    scope = os.getenv("CH_SECRET_SCOPE")
    if scope:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark).secrets.get(scope, os.getenv("CH_SECRET_KEY", "api_key"))
    raise RuntimeError("Set COMPANIES_HOUSE_API_KEY or CH_SECRET_SCOPE")


def run(companies: list[str], catalog: str, schema: str) -> int:
    import requests
    from pyspark.sql import SparkSession, Window, functions as F
    from pyspark.sql.types import (
        LongType, StringType, StructField, StructType, TimestampType
    )

    for identifier in (catalog, schema):
        if not IDENTIFIER_RE.fullmatch(identifier):
            raise ValueError("Use simple catalog/schema identifiers")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    table = f"`{catalog}`.`{schema}`.`psc_events`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {table} (
        psc_uri STRING, company_number STRING, event_type STRING,
        event_timepoint BIGINT, event_timestamp TIMESTAMP,
        ingested_at TIMESTAMP, payload_json STRING
      ) USING DELTA
    """)

    previous_rows = (
        spark.table(table)
        .where(F.col("company_number").isin(companies))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("psc_uri").orderBy(F.col("event_timepoint").desc())
            ),
        )
        .where("rn = 1")
        .select("psc_uri", "company_number", "payload_json", "event_type")
        .collect()
    )
    previous = {
        row["psc_uri"]: (row["company_number"], row["payload_json"], row["event_type"])
        for row in previous_rows
    }

    session = requests.Session()
    session.auth = (resolve_api_key(spark), "")
    session.headers.update({"User-Agent": "UK-Corporate-Intelligence-Research/0.1"})

    fetched: dict[str, tuple[str, str]] = {}
    for company in companies:
        fetched.update(fetch_company_pscs(session, company))
        if len(fetched) > MAX_RECORDS:
            raise ValueError("Too many PSC records for this cohort MVP")

    changes = plan_changes(fetched, previous)
    if not changes:
        print(f"Checked {len(companies)} companies; no new PSC states")
        return 0

    # A synthetic, increasing *poll position*, not a Companies House stream offset.
    top_row = spark.table(table).agg(F.max("event_timepoint").alias("max_tp")).first()
    last_timepoint = top_row["max_tp"] if top_row else None
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    timepoint = max(int(time.time() * 1_000_000), int(last_timepoint or 0) + 1)
    schema_type = StructType([
        StructField("psc_uri", StringType(), False),
        StructField("company_number", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timepoint", LongType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("payload_json", StringType(), True),
    ])
    rows = [(uri, company, kind, timepoint, now, now, payload)
            for uri, company, kind, payload in changes]
    spark.createDataFrame(rows, schema=schema_type).write.format("delta").mode(
        "append"
    ).saveAsTable(table)
    counts = {kind: sum(1 for x in changes if x[2] == kind)
              for kind in ("baseline", "changed", "deleted")}
    print(f"Checked {len(companies)} companies; wrote {len(rows)} events: {counts}")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--companies", required=True, help="Comma-separated company numbers")
    parser.add_argument("--catalog", default="uk_corporate")
    parser.add_argument("--schema", default="bronze")
    args = parser.parse_args()
    run(parse_companies(args.companies), args.catalog, args.schema)


if __name__ == "__main__":
    main()
