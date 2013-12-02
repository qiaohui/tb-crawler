#!coding=utf-8

import os
import sys
from pygaga.helpers.urlutils import get_cookie_value

def get_gtk(skey):
    """
    >>> get_gtk("@5s1PMsmrz")
    1943850567
    """
    hashkey = 5381
    for s in skey:
        hashkey += (hashkey << 5) + ord(s)
    return hashkey & 0x7fffffff

if __name__ == "__main__":
    "usage: python utils.py -v"
    import doctest
    doctest.testmod()

