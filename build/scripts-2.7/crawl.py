#!/Library/Frameworks/Python.framework/Versions/2.7/Resources/Python.app/Contents/MacOS/Python
# coding: utf-8

import daemon
import gflags
import sys
import time
import logging
import traceback

from pygaga.helpers.logger import log_init
from guang_crawler.crawl_image_impl import crawl_image_main
from guang_crawler.crawl_item_impl import crawl_item_main

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_boolean('daemon', False, "run as daemon")
gflags.DEFINE_string('pidfile', "/var/run/crawl.py.pid", "daemon pid file")

gflags.DEFINE_string('path', "/space/wwwroot/image.guang.j.cn/ROOT/images/", "image path")
gflags.DEFINE_string('org_path', "/space/wwwroot/image.guang.j.cn/ROOT/org_images/", "org image path")
gflags.DEFINE_string('crawl_path', "/space/crawler/image_crawler/static", "image path")

gflags.DEFINE_integer('numid', 0, "crawl taobao num id")
gflags.DEFINE_integer('itemid', 0, "crawl item id")
gflags.DEFINE_integer('limit', 0, "limit crawl items count")
gflags.DEFINE_string('where', "", "additional where sql, e.g. a=b and c=d")
gflags.DEFINE_boolean('all', False, "crawl all items")
gflags.DEFINE_boolean('pending', False, "crawl pending items")
gflags.DEFINE_boolean('changed', False, "crawl items that changed recent")
gflags.DEFINE_boolean('commit', True, "is commit data into database?")
gflags.DEFINE_boolean('removetmp', False, "is remove temperary image files after crawl?")
gflags.DEFINE_boolean('force', False, "is force crawl offline items?")
gflags.DEFINE_boolean('debug_parser', False, "debug html parser?")

gflags.DEFINE_boolean('update_comments', False, "is update comments?")
gflags.DEFINE_integer('max_comments', 0, "max comments crawled")
gflags.DEFINE_boolean('update_main', True, "is update price, desc and images?")

gflags.DEFINE_boolean('clean_redis', False, "is clean redis comments then recrawl?")
gflags.DEFINE_string('redishost', "127.0.0.1", "redis host")
gflags.DEFINE_integer('redisport', 9089, "redis port")

gflags.DEFINE_integer('shopid', 0, "crawl shop id")
gflags.DEFINE_integer('interval', 0, "crawl interval between items in ms")
gflags.DEFINE_boolean('crawl_offline', False, "is crawl offline items?")
gflags.DEFINE_boolean('dump', False, "dump html content?")

if __name__ == "__main__":
    log_init(["CrawlLogger","urlutils"], "sqlalchemy.*")
    if FLAGS.daemon:
        daemon.daemonize(FLAGS.pidfile)
    while True:
        try:
            crawl_item_main()
        except:
            logger.error("fatal error %s", traceback.format_exc())
        time.sleep(60)
        try:
            crawl_image_main()
        except:
            logger.error("fatal error %s", traceback.format_exc())

