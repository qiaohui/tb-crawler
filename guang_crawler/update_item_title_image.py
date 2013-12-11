#! /usr/bin/env python
# coding: utf-8

"""
    定期更新商品的主图和标题
    商品主图 @ nfs
    10-10 增加历史记录功能
"""
import cStringIO
import gflags
import os
import logging
import sys
import time
import traceback
import Image
import math
import re

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download
from guang_crawler.taobao_api import get_taobao_items, get_top

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_integer('itemid', 0, "update item id")
gflags.DEFINE_integer('shopid', 0, "update shop id")
gflags.DEFINE_boolean('all', False, "update all items")
gflags.DEFINE_integer('limit', 500000, "Batch counts")
gflags.DEFINE_boolean('force', False, "is update offline shops?")
gflags.DEFINE_boolean('commit', False, "is commit data into database?")
gflags.DEFINE_boolean('forcibly', False, "is forcibly update title and image?")
gflags.DEFINE_boolean('img_update', False, "is update main image?")
gflags.DEFINE_string('crawl_path', "/space/wwwroot/image.guang.j.cn/ROOT/images/", "is upload to nfs?")

gflags.DEFINE_boolean('shop_activity', False, "is shop Activity?")
gflags.DEFINE_string('activity_shop_ids', "", "activity shop ids: 1,2,3,4")

gflags.DEFINE_boolean('check_image', False, "check all item image")

DEFAULT_UA="Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"

def update_all_shop():
    if FLAGS.force:
        return update_item("select i.id, i.num_id, i.shop_id, i.title, i.pic_url, i.local_pic_url, i.volume from item i, shop where i.shop_id = shop.id and shop.type <= 2 and i.manual_set=0 order by i.id desc limit %s" % FLAGS.limit)
    else:
        return update_item("select i.id, i.num_id, i.shop_id, i.title, i.pic_url, i.local_pic_url, i.volume from item i, shop where i.status=1 and i.shop_id = shop.id and shop.type <= 2 and shop.status=1 and i.manual_set=0 order by i.id desc limit %s" % FLAGS.limit)

def update_one_shop(shopid):
    if FLAGS.force:
        return update_item("select i.id, i.num_id, i.shop_id, i.title, i.pic_url, i.local_pic_url, i.volume from item i, shop where i.shop_id = shop.id and shop.type <= 2 and shop.id=%s and i.manual_set=0" % shopid)
    else:
        return update_item("select i.id, i.num_id, i.shop_id, i.title, i.pic_url, i.local_pic_url, i.volume from item i, shop where i.status=1 and i.shop_id = shop.id and shop.type <= 2 and shop.status=1 and shop.id=%s and i.manual_set=0" % shopid)

def update_one_item(itemid):
    if FLAGS.force:
        return update_item("select id, num_id, shop_id, title, pic_url, local_pic_url, volume from item where id=%s" % itemid)
    else:
        return update_item("select id, num_id, shop_id, title, pic_url, local_pic_url, volume from item where status=1 and id=%s" % itemid)

def update_activity_shop(activity_shop_ids):
    if activity_shop_ids:
        shop_ids = activity_shop_ids.split(",")
        for shop_id in shop_ids:
            update_one_shop(int(shop_id))

def check_image():
    #因为数据量比较大，分批检查
    db_limit = {
        1: 100000,
        100000: 200000,
        200000: 300000,
        300000: 400000,
        400000: 500000
    }
    n = 0
    for s, e in db_limit.items():
        sql = "select id, num_id, shop_id, pic_url, local_pic_url from item where status=1 limit %s,%s" % (s, e)
        db = get_db_engine()
        items = list(db.execute(sql))
        for item in items:
            item_id = item[0]
            item_iid = str(item[1])
            shop_id = item[2]
            pic_url = str(item[3])
            local_pic_url = str(item[4])
            validate_path = "/space/wwwroot/image.guang.j.cn/ROOT/images/" + str(shop_id) + "/big/" + local_pic_url
            if not os.path.exists(validate_path):
                n += 1
                logger.error("item %s not pic %s", item_id, validate_path)
                try:
                    download_image({'item_id': item_id, 'num_id': item_iid, 'shop_id': shop_id, 'pic_url': pic_url,
                                    'image_name': local_pic_url, 'crawl_path': FLAGS.crawl_path})
                except:
                    logger.error("download %s:%s failed reason: %s", item_id, pic_url, traceback.format_exc())
                    continue
    logger.info("stat item not image number=%s", n)

def update_item(sql):
    t = time.time()
    db = get_db_engine()
    item = db.execute(sql)

    results = get_taobao_items(get_top(), item, fn_join_iids=lambda
            x:','.join([str(i[1]) for i in x]), calllimit=60)

    for batch_item in results:
        for iid, item in batch_item.items.iteritems():
            item_id = item['req'][0]
            item_iid = item['req'][1]
            shop_id = item['req'][2]
            item_title = item['req'][3]
            item_picurl = item['req'][4]
            local_pic_url = item['req'][5]  #直接用数据库的文件名,不更新,类似"18142957186_28924096.jpg"
            volume = item['req'][6]

            if item['resp']:
                taobao_title = item['resp']['title']
                taobao_picurl = item['resp']['pic_url']
                taobao_volume = item['resp']['volume']

                #item_picurl != taobao_picurl,则需要重新获取，并存入dfs，再更新item
                #title, pic_url, pic_width, pic_height, modified

                if volume < taobao_volume:
                    db.execute("update item set volume=%s where id=%s", taobao_volume, item_id)
                    logger.info("item %s update volume from %s to %s", item_id, volume, taobao_volume)

                if FLAGS.forcibly:
                    #强制更新
                    is_title_update = True
                    is_picurl_update = True
                    # local_pic_url = "%s_%s.%s" % (item_iid, str(id(item)), item_picurl.split('.')[-1].split('?')[0].split('/')[-1])
                else:
                    if cmp(item_title, taobao_title):
                        is_title_update = True
                    else:
                        is_title_update = False

                    if cmp(item_picurl, taobao_picurl):
                        is_picurl_update = True
                    else:
                        is_picurl_update = False

                try:
                    if is_title_update:
                        if is_picurl_update:
                                width, height = download_image({'item_id': item_id, 'num_id': item_iid, 'shop_id': shop_id, 'pic_url': taobao_picurl, 'image_name': local_pic_url, 'crawl_path': FLAGS.crawl_path})
                                db.execute("update item set modified=now(), title=%s, pic_url=%s, pic_width=%s, pic_height=%s where id=%s", taobao_title, taobao_picurl, width, height, item_id)

                                logger.info("item %s num_id %s update title from %s to %s, pic_url from %s to %s", item_id, item_iid, item_title, taobao_title, item_picurl, taobao_picurl)
                        else:
                            db.execute("update item set modified=now(), title=%s where id=%s", taobao_title, item_id)

                            logger.info("item %s update title from %s to %s", item_id, item_title, taobao_title)

                        db.execute("insert into item_manual_operation (ref_id,action_type,old_value,new_value,operate_user) values (%s,'ROBOT_UPD_TITLE',%s,%s,-1)", item_id, item_title, taobao_title)
                    elif is_picurl_update:
                            width, height = download_image({'item_id':item_id, 'num_id': item_iid, 'shop_id': shop_id, 'pic_url': taobao_picurl, 'image_name': local_pic_url, 'crawl_path': FLAGS.crawl_path})
                            db.execute("update item set modified=now(), pic_url=%s, pic_width=%s, pic_height=%s where id=%s", taobao_picurl, width, height, item_id)

                            logger.info("item %s num_id %s update pic_url from %s to %s", item_id, item_iid, item_picurl, taobao_picurl)
                except:
                    logger.error("download %s:%s failed reason: %s", item_id, item_picurl, traceback.format_exc())
                    continue

    spent = time.time() - t
    logger.info("update_item_title_image use time : %s", spent * 1000)

def img_update():
    sql = "select id,num_id,shop_id,pic_url,local_pic_url from item where pic_url like '%%q90.%%'"
    db = get_db_engine()
    items = db.execute(sql)
    tr = re.compile("(.+\.(jpg|png))[^$]*.jpg$")
    for item in items:
        taobao_picurl = item[3]
        taobao_picurl = tr.sub(r'\1', taobao_picurl)
        try:
            width, height = download_image({'item_id':item[0], 'num_id': item[1], 'shop_id': item[2], 'pic_url': taobao_picurl, 'image_name': item[4], 'crawl_path': FLAGS.crawl_path})
            db.execute("update item set modified=now(), pic_url=%s, pic_width=%s, pic_height=%s where id=%s", taobao_picurl, width, height, item[0])
            logger.info("item %s update image ok", item[0])
        except:
            logger.error("download %s:%s failed reason %s", item[0], taobao_picurl, traceback.format_exc())
            continue


def download_image(kwargs):
    item_id = kwargs['item_id']
    num_id = kwargs['num_id']
    shop_id = kwargs['shop_id']
    crawl_path = kwargs['crawl_path']
    image_name = kwargs['image_name']
    pic_url = kwargs['pic_url']
    #先下主图，放到big目录
    headers = {
        'Referer': "http://item.taobao.com/item.htm?id=%s" % num_id,
        'User-Agent': DEFAULT_UA
    }
    shop_image_path = "%s/%s" % (crawl_path, shop_id)
    if not os.path.exists(shop_image_path):
        os.mkdir(shop_image_path)
    shop_image_big_path = "%s/big" % shop_image_path
    if not os.path.exists(shop_image_big_path):
        os.mkdir(shop_image_big_path)
    big_image_fullpath = "%s/%s" % (shop_image_big_path, image_name)

    data = download(pic_url, headers)
    f = open(big_image_fullpath, "w")
    f.write(data)
    f.close()

    try:
        image = Image.open(cStringIO.StringIO(open(big_image_fullpath).read()))
    except IOError, e:
        logger.info("Open image failed %s:%s %s", item_id, pic_url, e.message)

    if image.mode not in ('L', 'RGB'):
        image = image.convert('RGB')

    width, height = image.size

    #5个尺寸压缩图
    limit = {'sma': 60.0, 'mid': 210.0, 'mid2': 300.0, 'small2': 100.0, 'small3': 145.0}
    for k in limit:
        resize_image({'big_image_fullpath': big_image_fullpath, 'width': width, 'height': height, 'maxLimit': limit[k], 'father_path': k, 'shop_image_path': shop_image_path, 'image_name': image_name})

    return width, height

def resize_image(kwargs):
    big_image_fullpath = kwargs['big_image_fullpath']
    width = kwargs['width']
    height = kwargs['height']
    maxLimit = kwargs['maxLimit']
    shop_image_path = kwargs['shop_image_path']
    father_path = kwargs['father_path']
    image_name = kwargs['image_name']

    filepath = "%s/%s" % (shop_image_path, father_path)
    if not os.path.exists(filepath):
        os.mkdir(filepath)
    image_path_str = "%s/%s" % (filepath, image_name)

    ratio = 1.0
    if width > maxLimit or height > maxLimit:
        if height > width:
            ratio = maxLimit / height
        else:
            ratio = maxLimit / width

    width_dist = int(math.floor(width * ratio))
    height_dist = int(math.floor(height * ratio))
    convert_str = "convert -resize %sx%s -strip -quality 95 -density 72x72 -sharpen 0.1,0.4 %s %s" % (width_dist, height_dist, big_image_fullpath, image_path_str)
    os.system(convert_str)

def update_item_main():
    if FLAGS.all:
        update_all_shop()
    elif FLAGS.shopid > 0:
        update_one_shop(FLAGS.shopid)
    elif FLAGS.itemid > 0:
        update_one_item(FLAGS.itemid)
    elif FLAGS.shop_activity:
        update_activity_shop(FLAGS.activity_shop_ids)
    elif FLAGS.img_update:
        img_update()
    elif FLAGS.check_image:
        # 这里全量检查图片是否存在
        check_image()
    else:
        print 'Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS)

if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    update_item_main()
