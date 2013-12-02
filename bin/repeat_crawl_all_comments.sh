#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"
REDISHOST="192.168.32.15"

while [ true ]; do
    crawl_item.py --use_logfile --verbose info --changed --interval 2000 --update_comments --nocommit_html --redishost $REDISHOST --statshost $STATHOST --dbhost $DBHOST
    echosleep 60
    if [[ $? -ne 0 ]]; then
        break
    fi
done
