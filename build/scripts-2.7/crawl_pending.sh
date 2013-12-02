#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"
INTERVAL=1000
MAXCPU=3
WAITSEC=60

CRAWL_IMAGE=1
CRAWL_ITEM=1
REDIAL=0

while getopts ipr OPTION
do
    case ${OPTION} in
        i)CRAWL_ITEM=0
        ;;
        p)CRAWL_IMAGE=0
        ;;
        r)REDIAL=1
        ;;
    esac
done

while [ true ]; do
    if [[ $REDIAL -ne 0 ]]; then
        /usr/local/bin/redial
    fi
    if [[ $CRAWL_ITEM -ne 0 ]]; then
        crawl_item.py --verbose info --stderr --color --use_logfile --dbhost $DBHOST --pending --interval $INTERVAL --statshost $STATHOST
    fi
    if [[ $CRAWL_IMAGE -ne 0 ]]; then
        crawl_image.py --verbose info --stderr --color --use_logfile --dbhost $DBHOST --pending --parallel --max_cpu $MAXCPU --dnscache_retry_per_exception 10000 --removetmp --statshost $STATHOST
    fi
    echosleep $WAITSEC
    if [[ $? -ne 0 ]]; then
        break
    fi
done

