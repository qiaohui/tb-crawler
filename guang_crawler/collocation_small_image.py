#! /usr/bin/env python
# coding: utf-8

"""
    生成搭配功能中使用的100*100小图，压缩并裁剪，限定宽高，宽高比例不变
"""

import os
import glob
import Image
import logging
import datetime
import gflags

from pygaga.helpers import dateutils
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('CrawlLogger')
#logger.setLevel(logging.INFO)
#fh = logging.FileHandler('resizeImage.log')
#fh.setLevel(logging.INFO)
#logger.addHandler(fh)

FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('all', False, "collocation all image")
gflags.DEFINE_boolean('file_validate', False, "validate image file mtime")
gflags.DEFINE_boolean('db_validate', False, "validate item not small4 image, > 7 day")

def make_dirs_for_file(filename):
    filepath = "/".join(filename.split("/")[:-1])
    if not os.path.exists(filepath):
        try:
            os.makedirs(filepath)
        except:
            pass
    return filepath

def convert_img(fname, tname, width, height):
    if not os.path.exists(os.path.dirname(tname)):
        make_dirs_for_file(tname)
    if width > height:
        sys_str = "convert -strip -quality 95 -density 72x72 -resize x100 -gravity center -extent 100x100 %s %s" % (fname, tname)
    else:
        sys_str = "convert -strip -quality 95 -density 72x72 -resize 100x -gravity center -extent 100x100 %s %s" % (fname, tname)
    os.system(sys_str)

def validate_mtime(images, ts_now, ts_front):
    i = 0
    for f in images:
        try:
            # 第一次执行时，加all参数
            if FLAGS.all:
                i += 1
                image = Image.open(f)
                width, height = image.size

                f100 = f.replace("/big/", "/small4/")
                if not os.path.isfile(f100):
                    convert_img(f, f100, width, height)

                    logger.info("%s:%s", i, f100)
            else:
                #get file's mofidy time
                mt = os.path.getmtime(f)
                # 以后每隔30分钟执行
                if mt >= ts_front and mt <= ts_now:
                    i += 1
                    image = Image.open(f)
                    width, height = image.size

                    f100 = f.replace("/big/", "/small4/")
                    convert_img(f, f100, width, height)

                    logger.info("%s:%s", i, f100)
        except IOError, e:
            logger.error("Open image failed %s:%s %s", i, f, e.message)
            continue
    logger.info("convert image total: %s", i)

def file_validate():
    images = glob.glob("/space/wwwroot/image.guang.j.cn/ROOT/images/*/big/*.*")
    images_1 = glob.glob("/space/wwwroot/image.guang.j.cn/ROOT/images_1/*/big/*.*")

    now_time = datetime.datetime.now()
    ft = now_time - datetime.timedelta(minutes=32)
    ts_now = dateutils.date2ts(now_time)
    ts_front = dateutils.date2ts(ft)

    validate_mtime(images, ts_now, ts_front)
    validate_mtime(images_1, ts_now, ts_front)

def db_validate():
    db = get_db_engine()
    items = list(db.execute("select shop_id,local_pic_url from item where status=1 and crawl_status=2 and created between CURDATE()-interval 7 day and CURDATE()"))
    i = 0
    for item in items:
        shop_id = item[0]
        local_pic_url = item[1]
        if shop_id == 5:
            small4_path = "/space/wwwroot/image.guang.j.cn/ROOT/images_1/%s/small4/%s" % (shop_id, local_pic_url)
        else:
            small4_path = "/space/wwwroot/image.guang.j.cn/ROOT/images/%s/small4/%s" % (shop_id, local_pic_url)
        if not os.path.isfile(small4_path):
            i += 1
            try:
                big_path = small4_path.replace("/small4/", "/big/")
                image = Image.open(big_path)
                width, height = image.size
                convert_img(big_path, small4_path, width, height)

                logger.info("%s:%s", i, small4_path)
            except IOError, e:
                logger.error("Open image failed %s:%s %s", i, big_path, e.message)
                continue

if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    if FLAGS.file_validate:
        #每30分钟执行一次，根据文件的mtime检查
        file_validate()
    elif FLAGS.db_validate:
        #每7天执行一次，根据数据库数据的创建时间检查
        db_validate()


