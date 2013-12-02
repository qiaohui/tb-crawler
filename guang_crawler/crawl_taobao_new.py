#! /usr/bin/env python
# coding: utf-8

"""
    抓取淘宝商品，下载html，上下架，打term，抓主图
    注意：这里只从html中获取title,main_image

    shop.crawl_status   0=完成    1=等待        2=正在爬
    item.crawl_status   0=等待    1=完成html    2=完成图片
    item.status         1=在线    2=下线        3=黑名单
"""
import gflags
import logging
import time
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from guang_crawler.taobao_item import TaobaoItem
from guang_crawler.taobao_category import TaobaoCategory
from guang_crawler.taobao_term import TermFactory

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('all', False, "update all shop")
gflags.DEFINE_integer('shopid', 0, "update shop id")
gflags.DEFINE_integer('interval', 0, "crawl interval between items")
gflags.DEFINE_boolean('force', False, "is update offline shops?")
gflags.DEFINE_boolean('commit', False, "is commit data into database?")
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')
gflags.DEFINE_string('path', "/space/wwwroot/image.guang.j.cn/ROOT/images/", "is upload to nfs?")

SHOP_CRAWL_STATUS_NONE = 0
SHOP_CRAWL_STATUS_WAIT = 1
SHOP_CRAWL_STATUS_CRAWLING = 2
SHOP_CRAWL_STATUS_ERROR = 3

ITEM_STATUS_BLACKLIST = 3
ITEM_STATUS_OFFLINE = 2
ITEM_STATUS_ACTIVE = 1


def crawl_all_shop():
    if FLAGS.force:
        return crawl_shops("select id, type, url from shop where crawl_status=1 and type<=2")
    else:
        return crawl_shops("select id, type, url from shop where crawl_status=1 and status=1 and type<=2")

def crawl_shop(shopid):
    return crawl_shops("select id, type, url from shop where id=%s" % shopid)

def filterNumIds(db, shop_id, tb_numids_set):
    shop_items = list(db.execute("SELECT num_id, status FROM item WHERE shop_id = %s" % shop_id))
    # db on line, type=1
    db_on_numids = set()
    # db off line, type=2
    db_off_numids = set()
    # db dead, type=3
    db_dead_numids = set()
    for item in shop_items:
        if item[1] == 1:
            db_on_numids.add(item[0])
        elif item[1] == 2:
            db_off_numids.add(item[0])
        else:
            db_dead_numids.add(item[0])

    # 返回tb中有但是db online中没有的元素，这就是要新增或要重新上线的
    numids_set = tb_numids_set - db_on_numids
    # new
    new_numids = numids_set - db_off_numids
    # back on line
    back_online_numids = numids_set & db_off_numids

    #返回db中有但是tb中没有的元素，这就是要offline的
    offline_numids = db_on_numids - tb_numids_set

    return new_numids, back_online_numids, offline_numids, db_dead_numids

def crawl_shops(sql):
    db = get_db_engine()
    shops = list(db.execute(sql))

    if not shops:
        logger.info("not shop crawler.")
        return

    # debug
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()

    # global, all shop use
    tb_category = TaobaoCategory(db)
    term_factory = TermFactory(db)
    logger.info("init category %s and term factory %s.", len(tb_category.categories_dict), len(term_factory.all_terms))

    for shop in shops:
        crawl_one_shop(shop, tb_category, term_factory, db)

def crawl_one_shop(shop, tb_category, term_factory, db):
    shop_id = shop[0]
    shop_type = shop[1]
    shop_url = shop[2]

    # dsp 投放使用
    defaultCampaign = list(db.execute(
        "select id, default_uctrac_price from campaign where shop_id=%s and system_status = 1 and delete_flag = 0" % shop_id))
    if not defaultCampaign:
        logger.error("can not get the default campaign for shop: %s", shop_id)
        return

    """
        1.setting shop crawl_status=2
        2.crawler
        3.setting shop crawl_status=0
    """
    db.execute("update shop set crawl_status=%s where id=%s", SHOP_CRAWL_STATUS_CRAWLING, shop_id)

    # 店铺的所有商品num id,从新品列表抓取获得,这里注意:可能有多条记录
    allTbNumIds = list(db.execute("SELECT itemids FROM tb_shop_item WHERE shopid = %s", shop_id))
    tb_numids = []
    for ids in allTbNumIds:
        tb_numids.extend(ids[0].split(','))
    tb_numids_set = set(tb_numids)
    logger.info("crawling shop: %s %s, taobao online num %s", shop_id, shop_url, len(tb_numids_set))

    # 过滤
    new_numids, back_online_numids, offline_numids, db_dead_numids = filterNumIds(db, shop_id, tb_numids_set)
    logger.info("stat taobao shop %s: new_num:%s, back_online_num:%s, offline_num:%s" % (shop_id, len(new_numids), len(back_online_numids), len(offline_numids)))

    pic_down_failed_num = 0
    success_num = 0
    failed_num = 0
    offline_num = 0
    dead_num = 0
    if len(new_numids) > 0:
        for num_id in new_numids:
            if num_id in db_dead_numids:
                dead_num += 1
                continue

            try:
                item = TaobaoItem(shop_id, 0, num_id)
                if shop_type == 2:
                    item.is_tmall = True

                item.crawl_title()      # --->

                if not item.data:
                    failed_num += 1
                    logger.warning("crawler %s network connection failure", num_id)
                    continue
                if item.is_offline:
                    db.execute("update item set status=2, modified=now() where shop_id=%s and num_id=%s", shop_id, num_id)
                    logger.warning("crawler %s off line", num_id)
                    offline_num += 1
                    continue

                item.status = ITEM_STATUS_ACTIVE
                item.setCampaign(defaultCampaign)   # --->
                if item.cid:
                    item.category = tb_category.getCategoryPath(item.cid)
                item.termIds = item.matchTaobaoTerms(term_factory)    # --->
                item.setPicUrl()    # --->
                # 图片下载失败的，下次轮询再处理
                if not item.is_pic_download:
                    pic_down_failed_num += 1
                    continue

                item.db_create(db)
                success_num += 1

            except:
                failed_num += 1
                logger.error("crawling %s unknown exception %s", num_id, traceback.format_exc(), extra={'tags':['crawlItemException',]})
    logger.info("shop %s crawler: success %s, failed %s, offline %s, pic download failed %s, dead %s", shop_id,
                success_num, failed_num, offline_num, pic_down_failed_num, dead_num)

    if back_online_numids:
        db.execute("update item set status=1 where num_id in (%s)" % ', '.join("'" + str(s) + "'" for s in back_online_numids))
        logger.info("shop %s crawler: back online %s", shop_id, len(back_online_numids))

    if offline_numids:
        db.execute("update item set status=2 where num_id in (%s)" % ', '.join("'" + str(s) + "'" for s in offline_numids))
        logger.info("shop %s crawler: offline %s", shop_id, len(offline_numids))

    #抓取失败比较多的，重新抓取
    if failed_num < 5 or pic_down_failed_num < 5:
        db.execute("update shop set crawl_status=%s where id=%s", SHOP_CRAWL_STATUS_NONE, shop_id)
    else:
        db.execute("update shop set crawl_status=%s where id=%s", SHOP_CRAWL_STATUS_WAIT, shop_id)

    # 以下操作是供统计使用,type=0:新增,1:下架;2:上架
    if len(new_numids) > 0:
        for num_id in new_numids:
            db.execute("INSERT INTO item_status_record (num_id,type,create_time) VALUES (%s, %s, now())", num_id, 0)
    if len(back_online_numids) > 0:
        for num_id in back_online_numids:
            db.execute("INSERT INTO item_status_record (num_id,type,create_time) VALUES (%s, %s, now())", num_id, 2)
    if len(offline_numids) > 0:
        for num_id in offline_numids:
            db.execute("INSERT INTO item_status_record (num_id,type,create_time) VALUES (%s, %s, now())", num_id, 1)


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    if FLAGS.shopid > 0:
        crawl_shop(FLAGS.shopid)
    else:
        crawl_all_shop()
