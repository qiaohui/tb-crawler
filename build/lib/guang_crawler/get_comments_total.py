#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
import redis
import traceback

from pygaga.helpers.logger import log_init
from guang_crawler import comments_pb2
from pygaga.helpers.dbutils import get_db_engine
gflags.DEFINE_integer('itemid', 0, "crawl item id")

gflags.DEFINE_string('redishost', "192.168.32.103", "redis host")
gflags.DEFINE_integer('redisport', 9089, "redis port")
gflags.DEFINE_integer('char_limit', 5, "char limit")
gflags.DEFINE_boolean('all', False, "get all items comments num")
logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

def main():
    redcli = redis.Redis(FLAGS.redishost, FLAGS.redisport)
    key = "guang:rate:%s" % FLAGS.itemid
    l = redcli.llen(key)
    if l:
        data = redcli.lrange(key, 0, l)
    else:
        data = []
    logger.info("Total results: %s", l)
    for d in data:
        try:
            comments = comments_pb2.comments()
            comments.ParseFromString(d)
            logger.info("data: %s %s %s %s %s %s %s %s", comments.rateid, comments.userid, comments.result, comments.time, comments.userrank, comments.userviplevel, comments.user, comments.content)
        except:
            logger.error("    failed %s %s - %s", d.encode('hex'), d, traceback.format_exc())

def get_all():
    db = get_db_engine()
    items = db.execute("select id from item where status=1")

    if items:
        items_list = list(items)
        pool = redis.ConnectionPool(host = FLAGS.redishost, port = FLAGS.redisport)
        r = redis.Redis(connection_pool = pool)
        for item in items_list:
            itemid = item[0]
            key = "guang:rate:%s" % itemid
            l = r.llen(key)
            if l:
                data = r.lrange(key, 0, l)
            else:
                data = []
            row = 0
            good = 0
            poor = 0
            for d in data:
                try:
                    coms = comments_pb2.comments()
                    coms.ParseFromString(d)
                    content = coms.content
                    result = coms.result
                    if len(content) >= FLAGS.char_limit:
                        row += 1 
                        if result==1:
                            good += 1
                        else:
                            poor += 1
                except:
                    logger.error("    failed %s %s - %s", d.encode('hex'), d, traceback.format_exc())
            logger.info("%s data : %s %s %s",itemid, row, good, poor)
            db.execute("replace into item_comments_total(item_id,row,good,poor) values (%s,%s,%s,%s) " % (itemid,row,good,poor ))
        logger.info("total items : %s", len(items_list))
if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    if FLAGS.all:
        get_all()
    else:
        main()

