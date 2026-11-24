import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from db import init_db, last_snapshot_block, save_block_snapshots


def _subnet(netuid: int):
    rao = SimpleNamespace(rao=1_000)
    return SimpleNamespace(netuid=netuid, alpha_in=rao, alpha_out=rao, tao_in=rao, price=rao, moving_price=1.5)


class LastSnapshotBlockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.conn = init_db(Path(self.tmp.name) / "test.db")

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_empty_database_has_no_resume_point(self):
        self.assertIsNone(last_snapshot_block(self.conn))

    def test_returns_newest_snapshot_block(self):
        save_block_snapshots(self.conn, 120, [_subnet(1), _subnet(2)])
        save_block_snapshots(self.conn, 125, [_subnet(1)])
        save_block_snapshots(self.conn, 123, [_subnet(1)])
        self.assertEqual(last_snapshot_block(self.conn), 125)


if __name__ == "__main__":
    unittest.main()
