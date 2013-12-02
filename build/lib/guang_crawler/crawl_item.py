#!/usr/bin/env python
# coding: utf-8

import gflags
import sys

from pygaga.helpers.logger import log_init
from guang_crawler.crawl_item_impl import crawl_item_main

gflags.DEFINE_integer('numid', 0, "crawl taobao num id")
gflags.DEFINE_integer('itemid', 0, "crawl item id")
gflags.DEFINE_integer('shopid', 0, "crawl shop id")
gflags.DEFINE_integer('limit', 0, "limit crawl items count")
gflags.DEFINE_integer('interval', 0, "crawl interval between items in ms")
gflags.DEFINE_string('where', "", "additional where sql, e.g. a=b and c=d")
gflags.DEFINE_boolean('all', False, "crawl all items")
gflags.DEFINE_boolean('pending', False, "crawl pending items")
gflags.DEFINE_boolean('changed', False, "crawl items that changed recent")
gflags.DEFINE_boolean('commit', True, "is commit data into database?")
gflags.DEFINE_boolean('force', False, "is crawl offline items?")
gflags.DEFINE_boolean('debug_parser', False, "debug html parser?")

gflags.DEFINE_boolean('update_comments', False, "is update comments?")
gflags.DEFINE_integer('max_comments', 0, "max comments crawled")
gflags.DEFINE_boolean('update_main', True, "is update price, desc and images?")
gflags.DEFINE_boolean('dump', False, "dump html content?")

gflags.DEFINE_boolean('clean_redis', False, "is clean redis comments then recrawl?")
gflags.DEFINE_string('redishost', "127.0.0.1", "redis host")
gflags.DEFINE_integer('redisport', 9089, "redis port")

gflags.DEFINE_boolean('hotest', False, "is update hsdl-guang-bi-db1 item_hotest comments?")
gflags.DEFINE_string('bihost', "127.0.0.1", "bi1 host")
gflags.DEFINE_integer('mostPage', 20, "comment most page")

if __name__ == "__main__":
    log_init(["CrawlLogger","urlutils"], "sqlalchemy.*")
    crawl_item_main()
