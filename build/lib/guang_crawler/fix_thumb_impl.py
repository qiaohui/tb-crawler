# coding: utf-8

import cStringIO
import gflags
import os
import logging
import re
import sys
import signal
import traceback
import Image
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker

from pygaga.helpers.mapreduce_multiprocessing import SimpleMapReduce, identity
from pygaga.helpers.utils import make_dirs_for_file
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

def resql(sql):
    if FLAGS.limit:
        limit_sql = " limit %s" % FLAGS.limit
    else:
        limit_sql = ""
    if FLAGS.where:
        where_sql = " and %s " % FLAGS.where
    else:
        where_sql = " and 1 "
    return sql % (where_sql, limit_sql)

def fix_all_items():
    if FLAGS.force:
        return fix_items(resql("select item_id, width, height, item_images.url, item_images.local_url from item_images,item where item.id=item_images.item_id %s order by item_id desc %s"))
    else:
        return fix_items(resql("select item_id, width, height, item_images.url, item_images.local_url from item_images,item where item.id=item_images.item_id and item.status=1 %s order by item_id desc %s"))

def fix_one_item(item_id):
    if FLAGS.force:
        return fix_items("select item_id, width, height, item_images.url, item_images.local_url from item_images,item where item.id=item_images.item_id and item.id=%s" % item_id)
    else:
        return fix_items("select item_id, width, height, item_images.url, item_images.local_url from item_images,item where item.id=item_images.item_id and item.status=1 and item.id=%s" % item_id)

def transform_args(iter):
    for i in iter:
        yield ({'item':i, 'crawl_path':FLAGS.crawl_path, 'server_path':FLAGS.path, 'is_remove':FLAGS.removetmp, 'org_server_path':FLAGS.org_path,
            'dbuser':FLAGS.dbuser, 'dbpasswd':FLAGS.dbpasswd, 'dbhost':FLAGS.dbhost, 'dbport':FLAGS.dbport, 'db':FLAGS.db, 'echosql':FLAGS.echosql})

def fix_items(sql):
    db = get_db_engine()

    items = db.execute(sql)
    logger.info("Fixing image total %s", items.rowcount)
    if not items.rowcount:
        return
    if FLAGS.parallel:
        mapper = SimpleMapReduce(fix_item2, identity)
        results = mapper(transform_args(items))
        logger.info("fix finished %s", len(results))
    else:
        for item in items:
            fix_item2({'item':item, 'crawl_path':FLAGS.crawl_path, 'server_path':FLAGS.path, 'is_remove':FLAGS.removetmp, 'org_server_path':FLAGS.org_path})

def fix_item2(kwargs):
    #signal.signal(signal.SIGINT, signal.SIG_IGN)
    item = kwargs['item']
    crawl_path = kwargs['crawl_path']
    server_path = kwargs['server_path']
    org_server_path = kwargs['org_server_path']
    is_remove = kwargs['is_remove']
    #thumbs =  ((350,350),(710,10000))
    thumbs =  ((710,10000),)

    org_path = "%s%s" % (org_server_path, item[4])
    if not os.path.exists(org_path):
        logger.error("Org image not found :" + item[3] + " " + item[4])
    for width, height in thumbs:
        thumb_path = "%s%sx%s_%s" % (server_path, width, height, item[4])
        if not os.path.exists(thumb_path):
            logger.error("Thumb image not found :" + item[3] + " " + item[4])
            # fix it
            try:
                image = Image.open(cStringIO.StringIO(open(org_path).read()))
                if image.mode not in ('L', 'RGB'):
                    image = image.convert('RGB')
                if width != 710:
                    image.thumbnail((width, height), Image.ANTIALIAS)
                elif item[1] < 710:
                    image.thumbnail((710, 710*item[2]/item[1]), Image.ANTIALIAS)
                make_dirs_for_file(thumb_path)
                thumbfile = open(thumb_path, "w")
                image.save(thumbfile, "JPEG")
                thumbfile.close()
                logger.info("fix suc!")
            except:
                logger.info("generate thumb failed %s %s %sx%s error : %s", item[0], thumb_path, width, height, traceback.format_exc())

def fix_thumb_main():
    if FLAGS.all:
        fix_all_items()
    elif FLAGS.itemid > 0:
        fix_one_item(FLAGS.itemid)
    else:
        print 'Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS)
