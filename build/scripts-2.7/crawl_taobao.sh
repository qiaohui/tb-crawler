#!/bin/sh

DBHOST="192.168.32.10"

while [ true ]; do
    crawl_taobao.py --use_logfile --verbose info --stderr --color --interval 1500 --all --dbhost $DBHOST
    echosleep 300
    if [[ $? -ne 0 ]]; then
        break
    fi
done

