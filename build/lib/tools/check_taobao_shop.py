#!/usr/bin/env python
# coding: utf-8

import gflags
import os
import logging
import sys
import traceback
import urlparse

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_string('path', "/Users/chris/workspace/taobao-crawler/guang_crawler/taobao_shop_name_crown.txt", "path")

def crawl_main():
    hosts = set()
    hosts_in_db = set()
    hosts_attr = {}

    db = get_db_engine()
    result = db.execute("select url from shop")

    for row in result:
        hosts_in_db.add(str(urlparse.urlparse(row[0]).netloc))

    #print hosts_in_db
    for line in open(FLAGS.path):
        url = line.split()[0]
        host = str(urlparse.urlparse(url).netloc)
        hosts.add(host)
        if url.find('tmall.com') > 0:
            shopname = " ".join(line.split()[1:])
        else:
            shopname = " ".join(line.split()[1:-1])
        hosts_attr[host] = shopname

    hosts_not_in_db = hosts - hosts_in_db
    print "hosts %s indb %s notindb %s" % (len(hosts), len(hosts_in_db), len(hosts_not_in_db))
    for host in hosts_not_in_db:
        print "http://%s/ %s" % (host, hosts_attr[host])

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    crawl_main()
