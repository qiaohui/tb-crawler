#!/usr/bin/env python
# coding: utf-8

import sys

import pHash

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "usage: ${prog} src dst"
        sys.exit(0)
    d1 = pHash.image_digest(sys.argv[1], 1.0, 1.0, 180)
    d2 = pHash.image_digest(sys.argv[2], 1.0, 1.0, 180)
    print 'digest', pHash.crosscorr(d1, d2)[1]
    h1 = pHash.imagehash(sys.argv[1])
    h2 = pHash.imagehash(sys.argv[2])
    print 'hash', pHash.hamming_distance(h1, h2)

