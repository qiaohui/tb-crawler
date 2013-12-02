#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"

/usr/local/bin/crawl_shop.py --stderr --color --verbose debug --where "id in (4,5,15,18)" --all --statshost $STATHOST --dbhost $DBHOST

