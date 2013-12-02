#!/bin/bash

DBHOST="192.168.32.10"

ps aux |grep "crawl_taobao_new.py --use_logfile"|grep -v "grep"
if [ $? -ne 0 ]; then
    nohup /usr/local/bin/crawl_taobao_new.py --use_logfile --verbose info  --interval 2000 --all --dbhost $DBHOST &
else
    exit 0
fi

