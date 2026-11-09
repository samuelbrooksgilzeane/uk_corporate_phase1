# UK Corporate Intelligence — National ingest and dbt model pack

This project uses the **official PSC bulk snapshot** and the **Companies House streaming API**, then stages the investigation-relevant read-only domains in dbt on Databricks.

## What is implemented

| Domain | Ingestion | dbt model |
|---|---|---|
| PSC nationwide baseline | Split ZIP snapshot | `stg_psc_bulk`, `int_psc_daily_deltas`, `dim_psc_history_bulk` |
| PSC changes | PSC stream | `stg_ch_psc_stream`; optional verified replay model |
| Company profiles | `companies` stream | `stg_ch_companies`, `dim_ch_companies` |
| Officers/appointments | `officers` stream | `stg_ch_officer_appointments` |
| Charges | `charges` stream | `stg_ch_charges` |
| Filing history | `filings` stream | `stg_ch_filings` |
| Insolvency cases | `insolvency` stream | `stg_ch_insolvency`, `fct_ch_insolvency_cases` |
| Disqualified officers | `disqualifications` stream | `stg_ch_disqualifications` |
| Company exemptions | `exemptions` stream | `stg_ch_exemptions` |
| PSC statements | `psc_statements` stream | `stg_ch_psc_statements` |
| Company-person relationships | dbt join | `bridge_ch_company_parties` |
| Investigation overview | dbt aggregation | `mart_ch_company_activity` |
| Ownership change monitoring | daily PSC snapshots | `mart_psc_changes_30d` |

These are **source-backed starter models**; they do not claim complete historical coverage of non-PSC streams. Search, submission, identity, document-download and write endpoints in the Developer Hub are different workflows, not independent national bulk tables; they are deliberately not modelled as though they were datasets.

## 1. Prerequisites

- Databricks Unity Catalog permissions to create schema, Delta tables and a managed Volume.
- A Databricks Python job runtime with PySpark and `requests`.
- Companies House **streaming API key** in a Databricks secret-backed environment variable `COMPANIES_HOUSE_STREAM_KEY`, or configure `CH_STREAM_SECRET_SCOPE` and `CH_STREAM_SECRET_KEY`.
- A dbt project using the Databricks adapter, with the `models/` and `tests/` directory copied across.

Do **not** put credentials in code or command-line flags.

## 2. One-time PSC bulk load

Find the archive date displayed at https://download.companieshouse.gov.uk/en_pscdata.html and run:

```bash
python ingestion/ingest_psc_bulk.py \
  --snapshot-date 2026-11-08 \
  --catalog uk_corporate \
  --schema bronze \
  --volume ch_ingestion
```

The date above is illustrative; replace it with the download page's actual available date.

The loader discovers all split ZIP parts, downloads **one at a time** into a Unity Catalog Volume, extracts NDJSON, and uses Spark to load the JSON records into Delta partitions. Each part is written with `replaceWhere`, so retrying a part overwrites only that part.

The manifest table marks a snapshot complete only after all archive parts have been committed. dbt ignores incomplete snapshots.

To test **one part only**, use `--max-parts 1`. This will **not** create an eligible baseline for the downstream models.

Bulk snapshots are large (currently dozens of split ZIP files), so run on an appropriately sized Databricks job, review cloud storage/compute cost, and avoid persisting temporary expanded JSON unnecessarily.

## 3. Capture the streaming PSC API

Register a Companies House **Streaming API** application, separate from a REST API application.

```bash
# First connection starts receiving NEW events from the present:
python ingestion/capture_ch_stream.py --stream psc --live-start

# Later runs resume the saved checkpoint:
python ingestion/capture_ch_stream.py --stream psc
```

It writes `bronze.ch_stream_events` and `bronze.ch_stream_checkpoints`.
The checkpoint is advanced only after the preceding batch was durably written. Replayed batches are deduplicated by `base_ch_stream_events`.

### Critical integrity gate: matching snapshot and stream

**Do not assume a downloaded public PSC ZIP supplies a matching stream timepoint.** The generic streaming guide describes snapshot offsets, but a public PSC bulk release may only provide generated-at metadata. An offset or calendar timestamp must not be guessed.

Until the cutover has been established and reconciled, keep:

- `dim_psc_history_bulk`: observed daily versions derived from complete bulk snapshots.
- `stg_ch_psc_stream`: separately captured event-level changes.

The integration model `dim_psc_history_verified` is **disabled by default**. Enable it only after the matching offset has been verified:

```yaml
vars:
  enable_psc_stream_union: true
  psc_baseline_date: '2026-11-08'
  psc_stream_cutover_timepoint: 123456789  # EXAMPLE ONLY, NEVER GUESS
```

The displayed offset is a placeholder. With a valid exact cutover, the model can replay baseline and subsequent PSC events by the stream's ordered timepoints.

For extra confidence, compare that replayed state to a later complete PSC snapshot: same record IDs, source etags, and deletion/cessation statuses. A discrepancy should block forensic risk outputs until investigated.

## 4. Capture the other public streams

```bash
python ingestion/capture_ch_stream.py --stream companies --live-start
python ingestion/capture_ch_stream.py --stream officers --live-start
python ingestion/capture_ch_stream.py --stream charges --live-start
python ingestion/capture_ch_stream.py --stream filings --live-start
python ingestion/capture_ch_stream.py --stream insolvency --live-start
python ingestion/capture_ch_stream.py --stream disqualifications --live-start
python ingestion/capture_ch_stream.py --stream exemptions --live-start
python ingestion/capture_ch_stream.py --stream psc_statements --live-start
```

**These are examples, not instructions to launch nine simultaneous jobs.** Companies House documents a maximum of two concurrent streaming connections per account. Schedule within that limit, monitor each stream's offset age and `416` responses, and plan separate backfills for anything missed before live capture began.

The stream alone supplies **changes, not a complete national baseline**. In particular, do not treat an empty officers stream as evidence that a company has no officers.

## 5. Run dbt

```bash
dbt debug
dbt build
```

The new models use `source()` for Bronze tables and `ref()` for other models. They preserve source record IDs and avoid name-only person matching. The company activity mart exposes counts and coverage, not a fraud accusation.

**Data grain:** company profiles are one row per company, officer appointments one row per source appointment URI, charges one row per source charge URI, insolvency case one row per source case number within a company, PSC snapshots one row per PSC URI per complete snapshot date.

## 6. Limitations before production use

1. `int_psc_daily_deltas` incrementally compares each new complete snapshot with its predecessor and retains only changed or missing records. `dim_psc_history_bulk` rebuilds from this much smaller change log. At national scale, optimise partitioning and later incrementally maintain the final SCD2 table too.
2. If an older snapshot is imported late or a processed snapshot is corrected, the incremental event model needs a deliberate backfill/full-refresh strategy; do not silently skip it.
3. A daily snapshot shows changes *between observed days*, not every change in between. The verified stream replay is for event-level fidelity.
4. Only complete, successfully ingested PSC snapshots become available to dbt. If a ZIP is malformed or its part listing incomplete, the loader stops.
5. Non-PSC sources capture only updates since their starting offset. Full historical coverage requires independent initial state backfills, which this package does not claim to provide.
6. The **company-person bridge links source records**, not certified identities across companies. Do not join disqualified individuals to directors solely by name.
7. Privacy, licensing, source retention, access controls, and documented evidence lineage must be reviewed before publishing person-level data or risk scores.

## Official references

- Public Data API: https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference
- Streaming API: https://developer-specs.company-information.service.gov.uk/streaming-api/reference
- Stream guidance/limits: https://developer-specs.company-information.service.gov.uk/streaming-api/guides/overview
- PSC bulk download: https://download.companieshouse.gov.uk/en_pscdata.html
- Stream authentication: https://developer-specs.company-information.service.gov.uk/streaming-api/guides/authentication
