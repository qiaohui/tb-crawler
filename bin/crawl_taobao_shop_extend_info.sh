#!/bin/bash

DBHOST="192.168.32.10"

ps aux |grep "crawl_shop_basic_info.py --use_logfile"|grep -v "grep"
if [ $? -ne 0 ]; then
    nohup /usr/local/bin/crawl_shop_basic_info.py --use_logfile --verbose info --stderr --color --interval 1500 --dbhost $DBHOST &
else
    exit 0
fi
