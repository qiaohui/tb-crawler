#!/usr/bin/env python
# coding: utf-8
import random
import gflags
import logging
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('AppLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_string('dfsimg', '', "fastdfs img")
gflags.DEFINE_integer('itemid', 0, "itemid")

def remove():
    db = get_db_engine()
    key = db.execute("select uniq_url from item_images where item_id=%s and fastdfs_filename=%s", FLAGS.itemid, FLAGS.dfsimg)
    if not key.rowcount > 0:
        return
    else:
        key = list(key)
    result = db.execute("select id from item_images where uniq_url=%s and disabled=0", key[0])
    i = 0
    for r in result:
        sql = "update item_images set disabled=1 where id=%s" % r[0]
        print("deleting %s/%s %s", i, result.rowcount, sql)
        db.execute(sql)
        i+=1

def main():
    remove()

if __name__ == "__main__":
    log_init('AppLogger', "sqlalchemy.*")
    main()
