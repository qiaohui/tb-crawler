#!/usr/bin/env python
# coding: utf-8

#import MySQLdb
import gflags
import os
import logging
import re
import sys
import signal
import socket
import time
import traceback
import urllib2
from urllib2 import HTTPError, URLError
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import Image
import cStringIO

from pygaga.helpers.logger import log_init
from pygaga.helpers.cachedns_urllib import custom_dns_opener
from pygaga.helpers.utils import make_dirs_for_file

from pygaga.helpers.dbutils import get_db_engine

try:
    from guang_crawler.mapreduce import SimpleMapReduce, identity
    has_multiprocessing = True
except:
    has_multiprocessing = False

logger = logging.getLogger('ProcessItemLogger')

FLAGS = gflags.FLAGS
gflags.DEFINE_string('sql', "", "additional sql, e.g. where a=b and c=d")
gflags.DEFINE_string('path', "/space/wwwroot/image.guang.j.cn/ROOT/images/", "image path")
gflags.DEFINE_string('org_path', "/space/wwwroot/image.guang.j.cn/ROOT/org_images/", "org image path")
gflags.DEFINE_string('crawl_path', "/tmp", "image path")
gflags.DEFINE_boolean('dryrun', False, "not run command")
gflags.DEFINE_boolean('force', False, "skip check status")

DEFAULT_UA="Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
urllib2.install_opener(custom_dns_opener())

def process_all_items():
    db = get_db_engine()

    last_time = 0
    sql = "select id,shop_id,local_pic_url,pic_url,manual_set,manual_updated_columns,status,num_id,pic_height,pic_width from item " + FLAGS.sql
    items = db.execute(sql)
    i = 0
    for item in items:
        i += 1
        process_item(item, items.rowcount, i)

def save_image(image_filename, data):
    if not os.path.exists(os.path.dirname(image_filename)) and not FLAGS.dryrun:
        make_dirs_for_file(image_filename)
    if os.path.exists(image_filename) and not FLAGS.dryrun:
        os.remove(image_filename)
    if not FLAGS.dryrun:
        f = file(image_filename, "wb")
        f.write(data)
        f.close()

def imagemagick_resize(width, height, image_filename, thumb_filename):
    if not os.path.exists(os.path.dirname(thumb_filename)) and not FLAGS.dryrun:
        make_dirs_for_file(thumb_filename)
    if os.path.exists(thumb_filename) and not FLAGS.dryrun:
        os.remove(thumb_filename)

    x = 0.3
    y = 0.4
    cmd = "convert +profile \"*\" -interlace Line -quality 95%% -resize %sx%s -sharpen %s,%s %s %s" % (width,height,x,y,image_filename, thumb_filename)
    logger.info("running %s", cmd)
    if not FLAGS.dryrun:
        os.system(cmd)

def get_image_size(image_filename):
    image = Image.open(cStringIO.StringIO(open(image_filename).read()))
    return image.size

def process_item(item, total, cur):
    try:
        id,shop_id,local_pic_url,pic_url,manual_set,manual_updated_columns,status,num_id,pic_height,pic_width = item
        big_path = "%s%s/big/%s" % (FLAGS.path, shop_id, local_pic_url)
        mid2_path = "%s%s/mid2/%s" % (FLAGS.path, shop_id, local_pic_url)
        mid_path = "%s%s/mid/%s" % (FLAGS.path, shop_id, local_pic_url)
        sma_path = "%s%s/sma/%s" % (FLAGS.path, shop_id, local_pic_url)
        if os.path.exists(big_path) and pic_width == 0:
            size = get_image_size(big_path)
            logger.debug("update %s size %s" % (id, size))
            db = get_db_engine()
            db.execute("update item set pic_width=%s,pic_height=%s where id=%s" % (size[0], size[1], id))

        if status in (2,3) and not FLAGS.force:
            return
        if not os.path.exists(big_path):
            headers = {'Referer' : "http://item.taobao.com/item.htm?id=%s" % id, 'User-Agent' : DEFAULT_UA}
            data = crawl_page(num_id, pic_url, headers)
            # save to path
            logger.debug("crawling %s %s %s %s", cur, total, big_path, item)
            save_image(big_path, data)
        if not os.path.exists(mid2_path):
            logger.debug("thumbing %s %s %s %s", cur, total, mid2_path, item)
            imagemagick_resize(300, 300, big_path, mid2_path)
        if not os.path.exists(mid_path):
            logger.debug("thumbing %s %s", mid_path, item)
            imagemagick_resize(210, 210, big_path, mid_path)
        if not os.path.exists(sma_path):
            logger.debug("thumbing %s %s", sma_path, item)
            imagemagick_resize(60, 60, big_path, sma_path)
    except:
        logger.error("unknown error %s, %s", item, traceback.format_exc())

def crawl_page(item_id, url, headers):
    logger.debug("Crawling %s", url)
    data = ""
    try:
        req = urllib2.Request(url, headers=headers)
        u = urllib2.urlopen(req)
        data = u.read()
        u.close()
    except ValueError, e:
        logger.info("download %s:%s url value error %s", item_id, url, e.message)
    except HTTPError, e1:
        logger.info("download %s:%s failed http code: %s", item_id, url, e1.code)
    except URLError, e2:
        logger.info("download %s:%s failed url error: %s", item_id, url, e2.reason)
    except socket.timeout:
        logger.info("download %s:%s failed socket timeout", item_id, url)
    return data

if __name__ == "__main__":
    log_init("ProcessItemLogger")

    process_all_items()

