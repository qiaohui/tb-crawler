#!/usr/bin/env python
# coding: utf-8

import gflags
import os
import logging
import sys

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_string('path', "/Users/chris/workspace/taobao-crawler/tools/meilishuo_shops.txt", "path")

def crawl_tao123(shops):
    for line in open(FLAGS.path):
        try:
            line = line.strip()
            url = "http://www.meilishuo.com%s" % line
            html = download(url)
            html_obj = parse_html(html)
            shop_url = html_obj.xpath("//div[@class='shop_summary']/a/@href")
            logger.debug("processing %s -> %s", line, shop_url)
            shops.update(shop_url)
        except:
            logger.error("processing %s failed", line)

def crawl_main():
    shops = set([])
    crawl_tao123(shops)
    for shop in shops:
        print shop

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    crawl_main()

