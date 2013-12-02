#!/Library/Frameworks/Python.framework/Versions/2.7/Resources/Python.app/Contents/MacOS/Python
# coding: utf-8

import gflags
import sys

from pygaga.helpers.logger import log_init
from guang_crawler.crawl_image_impl import crawl_image_main

gflags.DEFINE_string('path', "/space/wwwroot/image.guang.j.cn/ROOT/images/", "image path")
gflags.DEFINE_string('org_path', "/space/wwwroot/image.guang.j.cn/ROOT/org_images/", "org image path")
gflags.DEFINE_string('crawl_path', "/space/crawler/image_crawler/static", "image path")
gflags.DEFINE_integer('itemid', 0, "crawl item id")
gflags.DEFINE_integer('numid', 0, "crawl item num id")
gflags.DEFINE_integer('limit', 0, "limit crawl items count")
gflags.DEFINE_string('where', "", "additional where sql, e.g. a=b and c=d")
gflags.DEFINE_boolean('all', False, "crawl all items")
gflags.DEFINE_boolean('pending', False, "crawl pending items")
gflags.DEFINE_boolean('commit', True, "is commit data into database?")
gflags.DEFINE_boolean('removetmp', False, "is remove temperary image files after crawl?")
gflags.DEFINE_boolean('force', False, "is force crawl?")
#gflags.DEFINE_boolean('uploadfastdfs', True, "is upload to fastdfs?")
#gflags.DEFINE_boolean('uploadnfs', False, "is upload to nfs?")
#gflags.DEFINE_boolean('uploadorg', True, "is upload origin image to nfs?")

if __name__ == "__main__":
    log_init(["CrawlLogger","urlutils"], "sqlalchemy.*")
    crawl_image_main()

