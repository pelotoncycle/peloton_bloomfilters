from tempfile import NamedTemporaryFile
from unittest import TestCase

from peloton_bloomfilter import SharedMemoryBloomFilter



class TestErrorRate(TestCase):

    def assert_p_error(self, p, errors, count=10000):
        with NamedTemporaryFile() as f:
            bf = SharedMemoryBloomFilter(f.name, count + 1, p)
            for v in xrange(count):
                bf.add(v)
            self.assertEquals(
                sum(v in bf for v in xrange(count, count*2)),
                errors)

        
    def test(self):

        self.assert_p_error(0.2, 1340)
        self.assert_p_error(0.15, 870)
        self.assert_p_error(0.1, 653)
        self.assert_p_error(0.05, 312)
        self.assert_p_error(0.01, 75)
        self.assert_p_error(0.001, 8)
        self.assert_p_error(0.0000001,0)
