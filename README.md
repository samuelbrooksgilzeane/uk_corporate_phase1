# UK Corporate Intelligence — Phase 1: PSC history

**For the national-scale PSC bulk snapshot, PSC streaming API, and the broader public Developer Hub models, start with [NATIONAL_PLATFORM.md](NATIONAL_PLATFORM.md).** The instructions below preserve the original small-cohort REST polling exercise as a cheaper learning fallback.

**Status:** The Databricks/dbt models and the cohort ingestion script are provided. This code has not been executed against your Databricks workspace or live Companies House account.

## Recommended first run: small company cohort

This is the safest inexpensive way to start learning with real Companies House data. It avoids pulling several gigabytes of national PSC snapshots before you have validated the pipeline.

1. In Databricks, create or select a Unity Catalog catalog (the examples use `uk_corporate`).
2. Create the Bronze table using `ingestion/bronze_table.sql`.
3. Add a Databricks secret containing your Companies House **API key**.
4. Configure the Python job's environment using either:
   - `COMPANIES_HOUSE_API_KEY` (secret-backed environment variable), or
   - `CH_SECRET_SCOPE` and `CH_SECRET_KEY` (default key name `api_key`).
5. Run the Python file task with a real, small cohort of company numbers:

```bash
python ingestion/ingest_psc_cohort.py \
  --companies 01234567,SC123456 \
  --catalog uk_corporate \
  --schema bronze
```

**Replace those illustrative company numbers with companies you intend to track.** The script accepts up to 100 companies and a maximum of 10,000 returned PSC records as deliberate MVP limits. Run daily, ideally at a consistent time.

The job requests the full paginated PSC list for every selected company, compares each record to its latest saved version, and appends only **new, changed or deleted** states to `uk_corporate.bronze.psc_events`. It *fails closed* on API errors: an incomplete response cannot become a false deletion.

Each poll has a synthetic `event_timepoint` (microseconds, monotonically increasing). **This is not an official Companies House stream position.** `event_timestamp` means *when our poll observed the current state*, not the exact time an ownership change legally took effect.

### Then run dbt

Point your existing dbt project at `models/` and `tests/` in this package, or configure a Databricks profile matching `dbt_project.yml`:

```bash
dbt debug
dbt build
```

Your resulting models:

- `stg_psc_events`: parse PSC JSON and type key fields.
- `int_psc_changes`: remove duplicate deliveries/unchanged consecutive states.
- `dim_psc_history`: event-sourced SCD2 with observed-date ranges, explicit deletes and source identifiers.
- `mart_active_pscs`: latest non-deleted records without a `ceased_on` date.

The `tests/` directory validates one current version per PSC and valid history bounds.

## Optional: collect the real PSC stream separately

`ingestion/capture_psc_stream.py` is an **experimental, isolated capture job**, not yet connected to the dbt SCD2 source.

The streaming API needs a **separate streaming API key** (REST and streaming keys are not interchangeable). Configure `COMPANIES_HOUSE_STREAM_KEY` or `CH_STREAM_SECRET_SCOPE` / `CH_STREAM_SECRET_KEY`.

```bash
python ingestion/capture_psc_stream.py \
  --companies 01234567,SC123456 \
  --live-start --max-minutes 60
```

Later runs for the same cohort omit `--live-start` and resume from the saved checkpoint.
It stores raw events in `uk_corporate.bronze.psc_stream_capture` and the latest committed offset in `uk_corporate.bronze.psc_stream_checkpoints`. Checkpoint updates happen **after** data is committed, so replays may duplicate raw events but will not intentionally skip them.

### Why we deliberately do not merge this stream into the dbt table yet

Companies House's generic streaming guide describes snapshot timepoints, but the public downloadable PSC ZIP is not documented consistently enough to assume a matching stream offset is present. We must validate the specific bulk snapshot's end position before combining it with stream records. Otherwise, the handoff can silently create gaps or misordered SCD2 histories.

For Phase 1, the REST cohort poll is **deterministic at polling frequency** and handles current-state changes safely, but cannot see multiple intermediate changes between polls. Live streaming with a verified cutover is a later enhancement.

## Bronze data contract

| Column | Meaning |
|---|---|
| `psc_uri` | Record URI, stable within a company; never match persons by name alone. |
| `company_number` | Text identifier, preserves leading zeros/letter prefixes. |
| `event_type` | `baseline`, `changed`, `deleted`. |
| `event_timepoint` | Monotonic poll sequence in this MVP. |
| `event_timestamp` | UTC time we observed the state. |
| `ingested_at` | UTC timestamp of the Bronze event write. |
| `payload_json` | PSC data JSON (the inner `data` object for streaming); NULL for deleted events. |

The source can contain ceased records. `ceased_on` is **not the same** as a deleted API record.

## Validation / tradeoffs

- Run pure Python tests: `python -m unittest discover -s ingestion/tests -v`.
- This snapshot comparison is a useful learning MVP, not a complete forensic audit trail. Intra-day changes are only visible with the validated streaming feed.
- Preserve both observed dates and source `notified_on`/`ceased_on` dates. Do not label observed dates as legal effective dates.
- The initial API response cannot recreate all earlier state versions. History begins at your first poll.
- `dim_psc_history` currently rebuilds from the event log, intentionally prioritizing correctness for a small cohort. National scale will require incremental SCD2 processing.
- Respect API limits and privacy requirements. The job intentionally does not log names, dates of birth or full PSC payloads.
- The optional streamer is scope-specific: changing the company cohort creates a *new* independent checkpoint. A first `--live-start` begins at the present, not the historical beginning.

## Sources

- Public PSC API: https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/persons-with-significant-control/list
- PSC snapshot: https://download.companieshouse.gov.uk/en_pscdata.html
- PSC streaming: https://developer-specs.company-information.service.gov.uk/streaming-api/reference/persons-with-significant-control/stream
- Streaming guide: https://developer-specs.company-information.service.gov.uk/streaming-api/guides/overview
