import tempfile
from unittest import TestCase

import shared_memory_bloomfilter


class TestSharedBloomfilter(TestCase):

    def setUp(self):
        self.fd = tempfile.TemporaryFile()
        self.bloomfilter = shared_memory_bloomfilter.SharedMemoryBloomFilter(self.fd.fileno(), 1000, 0.001)

    def tearDown(self):
        self.fd.close()

    def test_add(self):
        self.assertEqual(0, len(self.bloomfilter))
        self.assertNotIn("5", self.bloomfilter)
        self.assertFalse(self.bloomfilter.add("5"))
        self.assertIn("5", self.bloomfilter)
