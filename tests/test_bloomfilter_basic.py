import tempfile
from unittest import TestCase

import shared_memory_bloomfilter


class TestSharedBloomfilter(TestCase):

    def setUp(self):
        self.fd = tempfile.NamedTemporaryFile()
        self.bloomfilter = shared_memory_bloomfilter.SharedMemoryBloomFilter(self.fd.name, 50, 0.001)

    def tearDown(self):
        self.fd.close()

    def test_add(self):
        self.assertEqual(0, len(self.bloomfilter))
        self.assertNotIn("5", self.bloomfilter)
        self.assertFalse(self.bloomfilter.add("5"))
        self.assertEqual(1, len(self.bloomfilter))
        self.assertIn("5", self.bloomfilter)

    def test_capacity(self):
        for i in xrange(50):
            self.assertFalse(self.bloomfilter.add(i))
        for i in xrange(50):
            self.assertIn(i, self.bloomfilter)
        self.assertTrue(self.bloomfilter.add(50))
        for i in xrange(50):
            self.assertNotIn(i, self.bloomfilter)
        self.assertIn(50, self.bloomfilter)


    def test_sharing(self):
        print "Test started\n"
        bf1 = self.bloomfilter
        bf2 = shared_memory_bloomfilter.SharedMemoryBloomFilter(self.fd.name, 50, 0.001)
        self.assertEquals(len(bf2), 0)
        self.assertNotIn(1, bf1)
        self.assertNotIn(1, bf2)

        bf1.add(1)
        
        self.assertIn(1, bf1)
        self.assertIn(1, bf2)

        bf2.add(2)
        self.assertIn(2, bf1)
        self.assertIn(2, bf2)

        
    def test_capacity_in_sync(self):
        bf1 = self.bloomfilter
        bf2 = shared_memory_bloomfilter.SharedMemoryBloomFilter(self.fd.name, 50, 0.001)
        bfs = [bf1, bf2]
        for i in xrange(50):
            bfs[i % 2].add(i)
        for i in xrange(50):
            self.assertIn(i, bf1)
            self.assertIn(i, bf2)
        self.assertTrue(bf2.add(50))
        for i in xrange(50):
            self.assertNotIn(i, bf1)
            self.assertNotIn(i, bf2)

        self.assertIn(50, bf1)
        self.assertIn(50, bf2)

