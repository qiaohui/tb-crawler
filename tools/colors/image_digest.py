#!/usr/bin/env python
# coding: utf-8

import os
import sys

import daemon
import gflags
import logging

import pHash

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('AppLogger')

FLAGS = gflags.FLAGS

def main():
    db = get_db_engine()
    items = db.execute("select id, shop_id, local_pic_url, concat('/space/wwwroot/image.guang.j.cn/ROOT/images/', shop_id, '/mid2/', local_pic_url) as img_path from item where status=1 and %s order by id" % FLAGS.where)
    for item in items:
        img_path = item[3]
        if not os.path.exists(img_path) or img_path.endswith('.png'):
            logger.warn('skipping %s %s', item[0], item[3])
            continue
        try:
            logger.debug('processing %s %s', item[0], item[3])
            d = ','.join(map(str, pHash.image_digest(img_path, 1.0, 1.0, 180).coeffs))
            db.execute("insert ignore into item_image_digest (item_id, digest) values (%s, '%s')" % (item[0], d))
        except:
            pass

if __name__ == "__main__":
    # usage: ${prog} ip:port --daemon --stderr ...
    gflags.DEFINE_boolean('daemon', False, "is start in daemon mode?")
    gflags.DEFINE_string('where', '1', "additional where")
    log_init('AppLogger', "sqlalchemy.*")
    #if FLAGS.daemon:
    #    file_path = os.path.split(os.path.abspath(__file__))[0]
    #    daemon.daemonize(os.path.join(file_path, 'app.pid'))
    main()
