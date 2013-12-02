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
    crawl_item.py --verbose info --use_logfile --dbhost $DBHOST --pending --interval $INTERVAL --statshost $STATHOST
    echosleep $WAITSEC
    if [[ $? -ne 0 ]]; then
        break
    fi
done

