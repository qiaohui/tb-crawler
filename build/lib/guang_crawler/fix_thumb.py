#!/usr/bin/env python
# coding: utf-8

import gflags
import sys

from pygaga.helpers.logger import log_init
from guang_crawler.fix_thumb_impl import fix_thumb_main

gflags.DEFINE_string('path', "/space/wwwroot/image.guang.j.cn/ROOT/images/", "image path")
gflags.DEFINE_string('org_path', "/space/wwwroot/image.guang.j.cn/ROOT/org_images/", "org image path")
gflags.DEFINE_string('crawl_path', "/space/crawler/image_crawler/static", "image path")
gflags.DEFINE_integer('itemid', 0, "crawl item id")
gflags.DEFINE_integer('limit', 0, "limit crawl items count")
gflags.DEFINE_string('where', "", "additional where sql, e.g. a=b and c=d")
gflags.DEFINE_boolean('all', False, "crawl all items")
gflags.DEFINE_boolean('removetmp', False, "is remove temperary image files after crawl?")
gflags.DEFINE_boolean('force', False, "is force crawl?")

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    fix_thumb_main()

