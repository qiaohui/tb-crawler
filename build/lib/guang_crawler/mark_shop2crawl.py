#!/usr/bin/env python
# coding: utf-8

import gflags
import os
import logging
import sys
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_boolean('all', True, "Is crawl all shops")
gflags.DEFINE_boolean('dryrun', False, "Is dry run")
gflags.DEFINE_list('shopids', [], "crawl shopids")

def crawl_main():
    if FLAGS.shopids:
        cond = "id in (%s)" % ",".join(FLAGS.shopids)
    elif FLAGS.all:
        cond = "1"
    else:
        logger.error("Args error, run with --help to get more info.")
        sys.exit(0)
    db = get_db_engine()

    sql = "update shop set crawl_status=1 where %s" % cond
    if FLAGS.dryrun:
        logger.debug(sql)
    else:
        db.execute(sql)
    sql = "delete from tb_category where %s" % cond
    if FLAGS.dryrun:
        logger.debug(sql)
    else:
        db.execute(sql)
    logger.info("all shops are marked to crawl")

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    crawl_main()

