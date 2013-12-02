#!/usr/bin/env python
# coding: utf-8

import gflags
import os
import logging
import sys
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_string('path', "/Users/chris/workspace/taobao-crawler/tools/meilishuo_final_url2.txt", "path")

def crawl_main():
    for host in open(FLAGS.path):
        url = "http://%s" % (host.strip())
        try:
            html = download(url)
            #import pdb; pdb.set_trace()
            html_obj = parse_html(html, 'gbk')
            if url.find('tmall.com') > 0:
                shop_url = html_obj.xpath("//h3[@class='shop-title']/a/@href")[0]
                shop_name = html_obj.xpath("//h3[@class='shop-title']/a/text()")[0]
                print shop_url, shop_name.encode('utf8')
            else:
                shop_url = html_obj.xpath("//div[@class='shop-info-simple']/a/@href")[0]
                shop_name = html_obj.xpath("//div[@class='shop-info-simple']/a/text()")[0]
                shop_rank = html_obj.xpath("//span[@class='shop-rank']//img/@src")[0]
                #good_rate = html_obj.xpath("//li[@class='goodrate']/text()")[0]
                print shop_url, shop_name.encode('utf8'), shop_rank
        except KeyboardInterrupt:
            raise
        except:
            logger.warn("processing %s failed, %s", url, traceback.format_exc())
            #import pdb; pdb.set_trace()

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    crawl_main()
