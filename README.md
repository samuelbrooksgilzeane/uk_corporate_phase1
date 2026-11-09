# UK Corporate Intelligence (Phase 1)

dbt models on Databricks for Companies House data, built from two sources:

- The Companies House Streaming API (all nine public streams), captured by `ingestion/capture_ch_stream.py`.
- The PSC bulk snapshot, loaded by `ingestion/ingest_psc_bulk.py`. This gives a full national PSC baseline, which the stream can't provide on its own.

Setup, run order and known limitations are in [NATIONAL_PLATFORM.md](NATIONAL_PLATFORM.md).

**Status:** this code hasn't been run against a live Databricks workspace or Companies House account yet.

## Quick start

1. Load a complete PSC bulk snapshot (section 2 of NATIONAL_PLATFORM.md).
2. Start capturing the streams with `capture_ch_stream.py` (sections 3 and 4).
3. Run `dbt build`.

## Tests

- Python: `python -m unittest discover -s ingestion/tests -v`
- dbt: `dbt build` runs the schema tests plus the checks in `tests/`.

## Things to keep in mind

- The streams only carry changes from when you start capturing. Apart from PSCs, which the bulk snapshot covers, a company with no records in a stream may still have them.
- History dates such as `valid_from_observed_on` are when we saw a change, not when it legally took effect. Use `notified_on` and `ceased_on` for the source dates.
- PSC and officer records are per company, so don't match people across companies by name.
- Review privacy, licensing and access controls before publishing any person-level data.
