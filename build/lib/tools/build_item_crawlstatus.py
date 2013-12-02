#!/usr/bin/env python
# coding: utf-8

import os
import sys

import daemon
import gflags
import logging

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine, get_rawdb_conn

logger = logging.getLogger('AppLogger')

FLAGS = gflags.FLAGS

def try_query(db, sql):
    try:
        db.query(sql)
    except:
        try:
            db.query(sql)
        except:
            db.query(sql)

def main():
    db = get_rawdb_conn()

    logger.debug("querying")
    db.query("select item_id, result, is_image_crawled, id from crawl_html where id>3000 order by id")
    results = db.store_result()

    i = 0
    db.autocommit(False)
    db.query("set autocommit=0;")
    for row in results.fetch_row(maxrows=0):
        item_id = row[0]
        result = row[1]
        is_image_crawled = row[2]
        i += 1
        if result == 1 and is_image_crawled == 1:
            try_query(db, "update item set crawl_status=2 where id=%s" % item_id)
        if result == 1 and is_image_crawled == 0:
            try_query(db, "update item set crawl_status=1 where id=%s" % item_id)
        if result == 0:
            try_query(db, "update item set crawl_status=0 where id=%s" % item_id)
        if i % 1000 == 0:
            logger.debug("processing %s %s %s/%s", row[3], item_id, i, 1194351)
            db.commit()
    db.commit()
    db.close()

if __name__ == "__main__":
    # usage:  ip:port --daemon --stderr ...
    gflags.DEFINE_boolean('daemon', False, "is start in daemon mode?")
    log_init('AppLogger', "sqlalchemy.*")
    #if FLAGS.daemon:
    #    file_path = os.path.split(os.path.abspath(__file__))[0]
    #    daemon.daemonize(os.path.join(file_path, 'app.pid'))
    main()

