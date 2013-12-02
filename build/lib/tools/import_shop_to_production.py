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

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_string('production_connstr', "mysql://guang:guang@127.0.0.1:3307/guang?charset=utf8", "production server connstr")

def convert_main():
    db = get_db_engine()
    db_production = get_db_engine(connstr=FLAGS.production_connstr)
    all_nicks = db_production.execute("select nick from shop");
    all_nick_set = set([row[0] for row in all_nicks])
    result = db.execute("select url, name from shop_shop where is_voted=1 and is_cloth=1 and is_delete=0;")
    for row in result:
        if row[0].find("tmall.com") > 0:
            shop_type = 2
        else:
            shop_type = 1
        if row[1] not in all_nick_set:
            db_production.execute("insert into shop(nick, url, type, status) values(%s, %s ,%s, 2)", row[1], row[0], shop_type)
        else:
            print row[1].encode('utf8'), " exists"

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    convert_main()

