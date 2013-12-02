#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"

/usr/local/bin/check_shop.py --stderr --color --verbose warning --all --statshost $STATHOST --dbhost $DBHOST


