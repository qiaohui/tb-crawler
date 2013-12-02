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

def crawl_tao123(shops):
    base_url = "http://www.meilishuo.com/shop/top/0/%s"
    end = 203
    for i in range(end):
        logger.debug("processing %s", i)
        url = base_url % i
        html = download(url)
        html_obj = parse_html(html)
        shops.update(html_obj.xpath("//div[@class='shop_item']//a/@href"))

def crawl_main():
    shops = set([])
    crawl_tao123(shops)
    for shop in shops:
        print shop

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    crawl_main()

