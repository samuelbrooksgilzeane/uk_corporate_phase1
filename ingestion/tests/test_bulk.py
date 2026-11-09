import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ingest_psc_bulk import discover_parts


class BulkTests(unittest.TestCase):
    def test_discovers_complete_set(self):
        html = '''<a href="psc-snapshot-2026-11-08_2of2.zip">2</a>
                  <a href="/psc-snapshot-2026-11-08_1of2.zip">1</a>'''
        result = discover_parts(html, '2026-11-08')
        self.assertEqual([index for index, _ in result], [1, 2])

    def test_rejects_incomplete_snapshot(self):
        with self.assertRaises(ValueError):
            discover_parts('<a href="psc-snapshot-2026-11-08_1of2.zip">1</a>', '2026-11-08')


if __name__ == '__main__':
    unittest.main()
