#! /usr/bin/env python
# coding: utf-8

"""
    调用淘宝api抓商品，上下架，打term，抓主图

    shop.crawl_status   0=完成    1=等待        2=正在爬
    item.crawl_status   0=等待    1=完成html    2=完成图片
    item.status         1=在线    2=下线        3=黑名单
"""
import gflags
import logging
import time
import traceback
import os

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from guang_crawler.taobao_api import get_taobao_items, get_taobaoke_top
from guang_crawler.taobao_item import TaobaoItem
from guang_crawler.taobao_category import TaobaoCategory
from guang_crawler.taobao_term import TermFactory

logger = logging.getLogger('TaobaoLogger')
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
        return crawl_shops("select id, type, url, term_limits from shop where crawl_status=1 and type<=2")
    else:
        return crawl_shops("select id, type, url, term_limits from shop where crawl_status=1 and status=1 and type<=2")

def crawl_shop(shopid):
    return crawl_shops("select id, type, url, term_limits from shop where id=%s" % shopid)

def imgExists(shop_id, local_pic_url):
    big_path = "%s/%s/big/%s" % (FLAGS.path, shop_id, local_pic_url)
    mid2_path = "%s/%s/mid2/%s" % (FLAGS.path, shop_id, local_pic_url)
    mid_path = "%s/%s/mid/%s" % (FLAGS.path, shop_id, local_pic_url)
    return os.path.isfile(big_path) and os.path.isfile(mid2_path) and os.path.isfile(mid_path)

def quickUpdatePrice(item_id, db):
    # 判断价格是不是在一天内被quick update过
    sql = "select UNIX_TIMESTAMP(time) as ts from price_update_track where item_id = %s" % item_id
    put_list = list(db.execute(sql))
    if len(put_list) > 0:
        db_time = long(put_list[0][0])
        now_time = long(round(time.time()))
        return now_time - db_time < 24 * 3600
    else:
        return False

def doCrawl(shop_id, numids_set):
    """
        注意：
            下面这3行完全是为了满足get_taobao_items的第二个参数限制，组装成类似数据库查询结果，没啥意义
    """
    num_iids = []
    for id in numids_set:
        num_iids.append((shop_id, id))

    # taobao.tbk.items.detail.get这个api有时间限制
    time.sleep(3)
    return_item_list = []
    results = get_taobao_items(get_taobaoke_top(), num_iids, fn_join_iids=lambda x: ','.join([str(i[1]) for i in x]))
    for r in results:
        for iid, item in r.items.iteritems():
            if item['resp']:
                return_item_list.append(dict(item['resp']))
    return return_item_list

def filterNumIds(db, shop_id, tb_numids_set):
    allNumIds = list(db.execute("SELECT num_id FROM item WHERE shop_id = %s AND status = 1" % shop_id))
    db_numids = []
    for numids in allNumIds:
        db_numids.extend(numids[0].split(','))
    db_numids_set = set(db_numids)

    #返回tb中有但是db中没有的元素，这就是要新增的, 也可能是重新上线的
    new_numids_set = tb_numids_set - db_numids_set
    #返回db中有但是tb中没有的元素，这就是要offline的
    offShelf_numids_set = db_numids_set - tb_numids_set
    #返回tb和db的公共元素，检查是否需要更新价格,标题,主图
    common_numids_set = tb_numids_set & db_numids_set

    return new_numids_set, offShelf_numids_set, common_numids_set

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
    logger.info("init category %s and term factory %s.", len(tb_category.categories_dict), len(term_factory.sub_terms))

    last_time = 0
    for shop in shops:
        cur = time.time() * 1000
        if cur - last_time < FLAGS.interval:
            time.sleep((FLAGS.interval - (cur - last_time)) / 1000.0)
        last_time = time.time() * 1000
        crawl_one_shop(shop, tb_category, term_factory, db)

def crawl_one_shop(shop, tb_category, term_factory, db):
    shop_id = shop[0]
    shop_type = shop[1]
    shop_url = shop[2]
    shop_termLimits = shop[3]

    # 白名单模式暂时没有使用上,shop.mode

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
    new_numids_set, offShelf_numids_set, common_numids_set = filterNumIds(db, shop_id, tb_numids_set)
    logger.info("stat taobao shop %s: new_num:%s, offline_num:%s, common_num:%s" % (shop_id, len(new_numids_set), len(offShelf_numids_set), len(common_numids_set)))

    new_items = []
    off2on_items = []
    black_num = 0
    pic_down_failed_num = 0
    if len(new_numids_set) > 0:
        new_item_list = doCrawl(shop_id, new_numids_set)
        if new_item_list:
            for dict_item in new_item_list:
                num_id = str(dict_item['num_iid'])
                #n_cid = dict_item['cid']
                tb_title = dict_item['title'].encode('utf-8')
                tb_detail_url = dict_item['item_url'].encode('utf-8')
                tb_price = float(dict_item['price'])
                if dict_item['pic_url']:
                    tb_pic_url = str(dict_item['pic_url'])
                else:
                    logger.warn("taobao item %s not pic_url", tb_detail_url)
                    continue
                volume = 0
                if dict_item.has_key('volume'):
                    volume = dict_item['volume']
                try:
                    #检查该商品是否重新上架
                    db_item = list(db.execute(
                        "select id, title, pic_url, local_pic_url, price, manual_set, status, category, volume from item where shop_id=%s and num_id='%s' and status!=1" % (shop_id, num_id)))
                    if db_item:
                        #update
                        db_volume = db_item[0][8]
                        #db_categroy = db_item[0][7]
                        db_status = int(db_item[0][6])
                        db_manual_set = int(db_item[0][5])
                        db_price = float(db_item[0][4])
                        db_local_pic_url = db_item[0][3]
                        db_pic_url = db_item[0][2]
                        db_title = db_item[0][1].encode('utf-8')
                        db_item_id = int(db_item[0][0])

                        if db_status == ITEM_STATUS_BLACKLIST:
                            black_num += 1
                            continue

                        item = TaobaoItem(shop_id, db_item_id, num_id)
                        item.status = ITEM_STATUS_ACTIVE     # 先置为上线状态，再检查其他属性是否有变化

                        if db_volume < volume:
                            item.volume = volume

                        #if not db_categroy:
                        #    item.category = tb_category.getCategoryPath(n_cid)

                        # 人工设置了图片和title
                        if db_manual_set == 1:
                            #检查价格
                            if tb_price != db_price and quickUpdatePrice(db_item_id, db):
                                item.price = tb_price
                            if not imgExists(shop_id, db_local_pic_url):
                                # 图片不存在，需要重新下载
                                item.detail_url = tb_detail_url
                                item.local_pic_url = db_local_pic_url
                                item.setPicUrl(tb_pic_url)
                                if not item.is_pic_download:
                                    pic_down_failed_num += 1
                                    continue
                        else:
                            if tb_title != db_title:
                                item.title = tb_title
                            if tb_price != db_price and quickUpdatePrice(db_item_id, db):
                                item.price = tb_price
                                # 图片路径有变化，或者原图片不存在了，都需要重新下载
                            if tb_pic_url != db_pic_url or not imgExists(shop_id, db_local_pic_url):
                                item.detail_url = tb_detail_url
                                item.local_pic_url = db_local_pic_url
                                item.setPicUrl(tb_pic_url)
                                if not item.is_pic_download:
                                    pic_down_failed_num += 1
                                    continue

                        # TODO
                        # dbItem是下线状态，可能要重新匹配terms，
                        # 原来下线时并没有删除对应的item_term, 但不排除其他渠道删除，以后有需求再处理
                        #

                        item.db_update(db)
                        off2on_items.append(num_id)
                    else:
                        #add
                        item = TaobaoItem(shop_id, 0, num_id)
                        item.title = tb_title
                        item.detail_url = tb_detail_url.replace("spm=(\\.|\\d)*", "spm=2014.12669715.0.0")
                        item.price = tb_price
                        item.volume = volume
                        #item.category = tb_category.getCategoryPath(n_cid)      # --->
                        item.termIds = item.matchTaobaoTerms(term_factory, str(shop_termLimits))    # --->
                        item.setPicUrl(tb_pic_url)          # --->
                        item.setCampaign(defaultCampaign)   # --->
                        item.status = ITEM_STATUS_ACTIVE

                        # 图片下载失败的，下次轮询再处理
                        if not item.is_pic_download:
                            pic_down_failed_num += 1
                            continue
                        item.db_create(db)
                        new_items.append(num_id)
                except:
                    logger.error("%s: %s creating failed %s", shop_id, num_id, traceback.format_exc())
                    continue
    logger.info("shop %s crawler: new %s, back on line %s, black %s, pic download failed %s", shop_id, len(new_items),
                len(off2on_items), black_num, pic_down_failed_num)

    if offShelf_numids_set:
        #offline
        db.execute("update item set status=2 where num_id in (%s)" % ', '.join("'" + str(s) + "'" for s in offShelf_numids_set))
    logger.info("shop %s crawler: offline %s", shop_id, len(offShelf_numids_set))

    db.execute("update shop set crawl_status=%s where id=%s", SHOP_CRAWL_STATUS_NONE, shop_id)

    # 以下操作是供统计使用,type=0:新增,1:下架;2:上架
    if len(new_items) > 0:
        for num_id in new_items:
            db.execute("INSERT INTO item_status_record (num_id,type,create_time) VALUES (%s, %s, now())", num_id, 0)
    if len(off2on_items) > 0:
        for num_id in off2on_items:
            db.execute("INSERT INTO item_status_record (num_id,type,create_time) VALUES (%s, %s, now())", num_id, 2)
    if len(offShelf_numids_set) > 0:
        for num_id in offShelf_numids_set:
            db.execute("INSERT INTO item_status_record (num_id,type,create_time) VALUES (%s, %s, now())", num_id, 1)


if __name__ == "__main__":
    log_init("TaobaoLogger", "sqlalchemy.*")
    if FLAGS.shopid > 0:
        crawl_shop(FLAGS.shopid)
    else:
        crawl_all_shop()
