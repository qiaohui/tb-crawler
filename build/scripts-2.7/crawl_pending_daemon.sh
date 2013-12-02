#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"
INTERVAL=1000
MAXCPU=3

cd /space/crawler
/usr/local/bin/crawl.py --verbose info --stderr --color --use_logfile --dbhost $DBHOST --pending --interval $INTERVAL --parallel --max_cpu $MAXCPU --dnscache_retry_per_exception 10000 --removetmp --daemon --statshost $STATHOST

