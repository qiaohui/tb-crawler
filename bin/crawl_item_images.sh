#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"
INTERVAL=1000
MAXCPU=3
WAITSEC=60

while [ true ]; do
    /usr/local/bin/redial
    echosleep 5
    if [[ $? -ne 0 ]]; then
        break
    fi
    crawl_image.py --verbose info --use_logfile --dbhost $DBHOST --pending --parallel --max_cpu $MAXCPU --dnscache_retry_per_exception 10000 --removetmp --statshost $STATHOST
    echosleep $WAITSEC
    if [[ $? -ne 0 ]]; then
        break
    fi
done

