import tempfile
import time
from pybloomfilter import BloomFilter

NS = 10**9
for _p in xrange(1,3):
    p = 10 ** _p
    for e in xrange(9):
        with tempfile.NamedTemporaryFile() as f:
            X = int(1000 * 10 ** (e / 2.0))
            print X, p, 
            name = f.name
            bloomfilter = BloomFilter(X + 1, 1.0/p, name)
            t = time.time()

            for x in xrange(X):
                bloomfilter.add(x)
            print (time.time() - t) / X * NS,
            t = time.time()
            for x in xrange(X):
                x in bloomfilter
            print (time.time() - t) / X * NS,
            t = time.time()
            for x in xrange(X, 2*X):
                x in bloomfilter
            print (time.time() - t ) / X * NS 

