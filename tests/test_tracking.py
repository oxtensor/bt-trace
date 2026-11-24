import unittest

from tracking import blocks_to_process


class BlocksToProcessTests(unittest.TestCase):
    def test_first_block_after_start_with_empty_database(self):
        self.assertEqual(blocks_to_process(None, 500, 100), ([500], 0))

    def test_next_consecutive_block(self):
        self.assertEqual(blocks_to_process(499, 500, 100), ([500], 0))

    def test_blocks_missed_while_busy_are_backfilled(self):
        # Processing block 500 took longer than a block, so the next wait
        # returned 503: blocks 501 and 502 must not be skipped.
        self.assertEqual(blocks_to_process(500, 503, 100), ([501, 502, 503], 0))

    def test_same_or_older_head_is_ignored(self):
        self.assertEqual(blocks_to_process(500, 500, 100), ([], 0))
        self.assertEqual(blocks_to_process(500, 498, 100), ([], 0))

    def test_gap_exactly_at_limit_is_fully_backfilled(self):
        blocks, dropped = blocks_to_process(400, 500, 100)
        self.assertEqual(blocks, list(range(401, 501)))
        self.assertEqual(dropped, 0)

    def test_gap_beyond_limit_keeps_newest_blocks(self):
        blocks, dropped = blocks_to_process(100, 500, 100)
        self.assertEqual(blocks, list(range(401, 501)))
        self.assertEqual(dropped, 300)  # blocks 101-400

    def test_nonpositive_limit_disables_cap(self):
        blocks, dropped = blocks_to_process(100, 500, 0)
        self.assertEqual(blocks, list(range(101, 501)))
        self.assertEqual(dropped, 0)


if __name__ == "__main__":
    unittest.main()
