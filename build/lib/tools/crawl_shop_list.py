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

gflags.DEFINE_string('path', "/space/wwwroot/image.guang.j.cn/ROOT/images/", "image path")

# TODO:
def crawl_dirtbshop(shops):
    base_url = "http://dirtbshop.com/list_shop_%s_1_1.html"
    end = 251
    for i in range(1, end+1):
        url = base_url % i
        html = download(url)
        html_obj = parse_html(html)
        import pdb; pdb.set_trace()
        urls = html_obj.xpath("//span[@class='grebtn_in']/a/@href")

def crawl_tao123(shops):
    base_url = "http://dianpu.tao123.com/nvzhuang/%s.php"
    end = 22
    for i in range(1, end+1):
        url = base_url % i
        html = download(url)
        html_obj = parse_html(html)
        shops.update(html_obj.xpath("//div[@class='cg_shop_info']//a/@href"))

def crawl_main():
    shops = set([])
    #crawl_tao123(shops)
    #crawl_dirtbshop(shops)
    for shop in shops:
        print shop

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    crawl_main()

