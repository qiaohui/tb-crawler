#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"
REDISHOST="192.168.32.15"
BIHOST="192.168.33.161"

while [ true ]; do
    sudo /usr/local/bin/redial
    echosleep 30
    if [[ $? -ne 0 ]]; then
        break
    fi
    crawl_item.py --verbose debug --use_logfile --interval 2000 --noupdate_main --hotest --mostPage 30 --update_comments --redishost $REDISHOST --dbhost $DBHOST --statshost $STATHOST --bihost $BIHOST
    echosleep 60
    if [[ $? -ne 0 ]]; then
        break
    fi
done

