"""Negative controls for provenance beyond independently updated file hashes."""
from pathlib import Path
import tempfile
import unittest

from patch_replay import inventory, round_trip


class PatchReplayTest(unittest.TestCase):
    def fixture(self, directory, before='upstream\n'):
        source = directory / 'source'
        source.mkdir()
        (source / 'value.txt').write_text(before)
        original = inventory(source)
        (source / 'value.txt').write_text('reviewed\n')
        patch = directory / 'fix.patch'
        patch.write_text('--- a/value.txt\n+++ b/value.txt\n@@ -1 +1 @@\n-upstream\n+reviewed\n')
        return source, patch, original

    def test_replays_changed_bytes_and_additions(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, patch, original = self.fixture(Path(temporary))
            (source / 'added.txt').write_text('addition\n')
            with patch.open('a') as stream:
                stream.write('--- a/added.txt\n+++ b/added.txt\n@@ -0,0 +1 @@\n+addition\n')
            round_trip(source, patch, original)
            self.assertEqual((source / 'value.txt').read_text(), 'reviewed\n')

    def test_updated_hashes_cannot_hide_wrong_upstream_origin(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, patch, original = self.fixture(Path(temporary), before='different origin\n')
            with self.assertRaisesRegex(ValueError, 'original inventory'):
                round_trip(source, patch, original)

    def test_unrecorded_addition_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, patch, original = self.fixture(Path(temporary))
            (source / 'unexplained.txt').write_text('unreviewed\n')
            with self.assertRaisesRegex(ValueError, 'not fully reversed'):
                round_trip(source, patch, original)


if __name__ == '__main__':
    unittest.main()
