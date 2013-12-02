#!/Library/Frameworks/Python.framework/Versions/2.7/Resources/Python.app/Contents/MacOS/Python
# coding: utf-8

"""
    部分商家的图片有log，将主图设置为详情页的第一张图，或其他
    避免2次下载，从fastdfs download
    为了不引起混乱，image resize到“/space/wwwroot/image.guang.j.cn/ROOT/images_1”,并将此目录挂载到nfs
"""
import gflags
import os
import logging
import traceback
import datetime
import urllib2, urllib

from pygaga.helpers.utils import make_dirs_for_file
from fdfs_client.client import Fdfs_client
from fdfs_client.exceptions import *
from pygaga.helpers.logger import log_init
from pygaga.helpers.urlutils import download
from pygaga.helpers.dbutils import get_db_engine
logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS
gflags.DEFINE_integer('itemid', 0, "replace item id")
gflags.DEFINE_integer('shopid', 0, "replace shop id")
gflags.DEFINE_boolean('all', False, "replace all items")
gflags.DEFINE_integer('number', 1, "item desc image number")
gflags.DEFINE_string('path', "/space/wwwroot/image.guang.j.cn/ROOT/images_1", "is upload to nfs?")

# shop对应详情页第几张图片
SHOP_NUM = {
    5: 1,   # 韩都衣舍
    11: 1,
    17: 1,
    149: 1
}

def replace_main():
    now_time = datetime.datetime.now()
    front_time = now_time - datetime.timedelta(hours=1)
    number = FLAGS.number  # 默认取详情页第一张图片

    fdfs_client = Fdfs_client('/etc/fdfs/client.conf')
    db = get_db_engine()

    if FLAGS.itemid > 0:
        item_sql = "select id,shop_id,local_pic_url from item where id=%s and status=1" % FLAGS.itemid
    elif FLAGS.shopid > 0:
        item_sql = "select id,shop_id,local_pic_url from item where shop_id=%s and crawl_status=2 and status=1" % FLAGS.shopid
    elif FLAGS.all:
        for shop, num in SHOP_NUM.items():
            number = num
            item_sql = "select id,shop_id,local_pic_url from item where shop_id=%s and crawl_status=2 and status=1 and created>'%s'" % (shop, front_time)

    items = list(db.execute(item_sql))
    logger.info("replace main image total %s", len(items))
    i = 1
    for item in items:
        item_id = item[0]
        shop_id = item[1]
        local_pic_url = item[2]

        # 一定要使用pos排序
        image_sql = "select item_id,fastdfs_filename,pos from item_images where type=2 and item_id=%s order by pos limit %s,1" % (item_id, number-1)
        image_item = list(db.execute(image_sql))
        try:
            if len(image_item) > 0 and image_item[0][0] is not None:
                fastdfs_filename = str(image_item[0][1])
            else:
                fastdfs_filename = "http://image2.guang.j.cn/images/%s/big/%s" % (shop_id, local_pic_url)
            download_image(fastdfs_filename, shop_id, item_id, local_pic_url, fdfs_client)
        except:
            logger.error("download %s:%s failed reason: %s", item_id, fastdfs_filename, traceback.format_exc())
            continue

        try:
            refreshCdnCache(shop_id, local_pic_url)
        except:
            logger.error("refreshCdnCache %s:%s failed: %s", item_id, local_pic_url, traceback.format_exc())
            continue
        logger.info("%s/%s replace item %s main image success %s", i, len(items), item_id, local_pic_url)
        i += 1

def download_image(pic_url, shop_id, item_id, local_pic_url, fdfs_client):
    big_path = "%s/%s/big/%s" % (FLAGS.path, shop_id, local_pic_url)
    mid2_path = "%s/%s/mid2/%s" % (FLAGS.path, shop_id, local_pic_url)
    mid_path = "%s/%s/mid/%s" % (FLAGS.path, shop_id, local_pic_url)
    sma_path = "%s/%s/sma/%s" % (FLAGS.path, shop_id, local_pic_url)
    sma2_path = "%s/%s/small2/%s" % (FLAGS.path, shop_id, local_pic_url)
    sma3_path = "%s/%s/small3/%s" % (FLAGS.path, shop_id, local_pic_url)
    headers = {
        'Referer': "http://www.j.cn/product/%s.htm" % item_id,
        'User-Agent': "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
    }

    # pic_url 包含http走下载，不包含即fastdfs get
    if "http://" in pic_url:
        data = download(pic_url, headers)
        save_image(big_path, data)
    else:
        try:
            if not os.path.exists(os.path.dirname(big_path)):
                make_dirs_for_file(big_path)

            fdfs_client.download_to_file(big_path, pic_url)
        except (ConnectionError, ResponseError, DataError), e:
            fastdfs_filename = "http://image2.guang.j.cn/images/%s/big/%s" % (shop_id, local_pic_url)
            data = download(fastdfs_filename, headers)
            save_image(big_path, data)
            logger.info("%s:%s fdfs get failed: %s, usage http download", item_id, pic_url, e)

    imagemagick_resize(300, 300, big_path, mid2_path)
    imagemagick_resize(210, 210, big_path, mid_path)
    imagemagick_resize(60, 60, big_path, sma_path)
    imagemagick_resize(100, 100, big_path, sma2_path)
    imagemagick_resize(145, 145, big_path, sma3_path)

def imagemagick_resize(width, height, image_filename, thumb_filename):
    # validate dir
    if not os.path.exists(os.path.dirname(thumb_filename)):
        make_dirs_for_file(thumb_filename)
    #validate file
    if not os.path.isfile(thumb_filename):
        cmd = "convert -resize %sx%s -strip -density 72x72 %s %s" % (width, height, image_filename, thumb_filename)
        os.system(cmd)

def save_image(image_filename, data):
    if not os.path.exists(os.path.dirname(image_filename)):
        make_dirs_for_file(image_filename)
    f = file(image_filename, "wb")
    f.write(data)
    f.close()

def refreshCdnCache(shop_id, local_pic_url):
    task = '{"urls":["http://image2.guang.j.cn/images_1/%s/mid2/%s"]}' % (shop_id, local_pic_url)
    data = {'username': 'langtaojin', 'password': 'LANGtaojin928', 'task': task}
    f = urllib2.urlopen(url='https://r.chinacache.com/content/refresh', data=urllib.urlencode(data))
    f.read()

if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    replace_main()


    """
    # test fdfs
    fdfs_client = Fdfs_client('/etc/fdfs/client.conf')
    # download_to_buffer
    bf = fdfs_client.download_to_buffer("g2/M01/81/56/wKggKlHwAuqjYHBSAAF5yY7ljV0648.jpg")
    f = open("/home/ldap//qiaohui.zhang/a.jpg","w")
    f.write(str(bf['Content']))
    f.close()

    # download_to_file
    try:
        ret_dict = fdfs_client.download_to_file("/home/ldap/qiaohui.zhang/a1.jpg","g2/M01/81/56/wKggKlHwAuqjYHBSAAF5yY7ljV0648.jpg")
        for key in ret_dict:
            print '[+] %s : %s' % (key, ret_dict[key])
    except (ConnectionError, ResponseError, DataError), e:
        print e
    """

