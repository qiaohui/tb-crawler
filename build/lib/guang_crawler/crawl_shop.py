#!/usr/bin/env python
# coding: utf-8

import gflags
import sys

from pygaga.helpers.logger import log_init
from guang_crawler.crawl_shop_impl import crawl_shop_main

gflags.DEFINE_integer('shopid', 0, "crawl shop id")
gflags.DEFINE_integer('limit', 0, "limit crawl shop count")
gflags.DEFINE_boolean('pending', False, "crawl pending items")
gflags.DEFINE_boolean('recent', False, "crawl recent failed pending items")
gflags.DEFINE_integer('recenthour', 12, "crawl shops that not update in recent hours")
gflags.DEFINE_integer('interval', 0, "crawl interval between items")
gflags.DEFINE_string('where', "", "additional where sql, e.g. a=b and c=d")
gflags.DEFINE_string('sql', "", "custom sql, select id,url,crawl_status,name from shop")
gflags.DEFINE_boolean('all', False, "crawl all shops")
gflags.DEFINE_boolean('commit', True, "is commit data into database?")
gflags.DEFINE_boolean('force', False, "is crawl offline shops?")
gflags.DEFINE_boolean('debug_parser', False, "debug html parser?")
gflags.DEFINE_boolean('dump', False, "dump html content?")

if __name__ == "__main__":
    log_init(["CrawlLogger","urlutils"], "sqlalchemy.*")
    crawl_shop_main()
