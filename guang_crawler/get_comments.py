#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
import redis
import sys
import traceback

from pygaga.helpers.urlutils import download
from pygaga.helpers.logger import log_init
from guang_crawler import comments_pb2

gflags.DEFINE_integer('itemid', 0, "crawl item id")

gflags.DEFINE_string('redishost', "127.0.0.1", "redis host")
gflags.DEFINE_integer('redisport', 9089, "redis port")

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
            logger.info("page=%s, crawltime=%s", comments.page, comments.crawltime)
            logger.info("data: %s %s %s %s %s %s %s %s", comments.rateid, comments.userid, comments.result, comments.time, comments.userrank, comments.userviplevel, comments.user, comments.content)
        except:
            logger.error("    failed %s %s - %s", d.encode('hex'), d, traceback.format_exc())

if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    main()

