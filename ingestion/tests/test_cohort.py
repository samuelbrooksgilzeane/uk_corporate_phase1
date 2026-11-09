import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ingest_psc_cohort import canonical_json, parse_companies, plan_changes, fetch_company_pscs


class CohortTests(unittest.TestCase):
    def test_paginated_psc_fetch(self):
        class FakeResponse:
            def __init__(self, data):
                self.status_code = 200
                self.data = data

            def raise_for_status(self):
                pass

            def json(self):
                return self.data

        class FakeSession:
            def get(self, _url, params, timeout):
                offset = params["start_index"]
                pages = {
                    0: {"total_results": 2, "items": [
                        {"links": {"self": "/company/01234567/persons-with-significant-control/individual/1"}, "name": "A"}
                    ]},
                    1: {"total_results": 2, "items": [
                        {"links": {"self": "/company/01234567/persons-with-significant-control/individual/2"}, "name": "B"}
                    ]},
                }
                return FakeResponse(pages[offset])

        self.assertEqual(len(fetch_company_pscs(FakeSession(), "01234567")), 2)

    def test_preserves_company_number_format(self):
        self.assertEqual(parse_companies("01234567,SC123456,01234567"), ["01234567", "SC123456"])

    def test_payload_json_is_canonical(self):
        self.assertEqual(canonical_json({"b": 2, "a": 1}), canonical_json({"a": 1, "b": 2}))

    def test_initial_events_and_unchanged_rerun(self):
        fetched = {"/p/1": ("01234567", '{"name":"Person"}')}
        events = plan_changes(fetched, {})
        self.assertEqual(events, [("/p/1", "01234567", "baseline", '{"name":"Person"}')])
        prior = {"/p/1": ("01234567", '{"name":"Person"}', "baseline")}
        self.assertEqual(plan_changes(fetched, prior), [])

    def test_changes_deletions_and_reappearances(self):
        previous = {
            "/p/1": ("01234567", '{"name":"Old"}', "changed"),
            "/p/2": ("01234567", '{"name":"Other"}', "baseline"),
        }
        fetched = {
            "/p/1": ("01234567", '{"name":"New"}'),
            "/p/3": ("01234567", '{"name":"New PSC"}'),
        }
        self.assertEqual(
            plan_changes(fetched, previous),
            [
                ("/p/1", "01234567", "changed", '{"name":"New"}'),
                ("/p/2", "01234567", "deleted", None),
                ("/p/3", "01234567", "changed", '{"name":"New PSC"}'),
            ],
        )
        self.assertEqual(
            plan_changes({"/p/2": ("01234567", '{"name":"Other"}')},
                         {"/p/2": ("01234567", None, "deleted")}),
            [("/p/2", "01234567", "changed", '{"name":"Other"}')],
        )


if __name__ == "__main__":
    unittest.main()
