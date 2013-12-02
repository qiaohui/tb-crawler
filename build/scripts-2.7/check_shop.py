#!/Library/Frameworks/Python.framework/Versions/2.7/Resources/Python.app/Contents/MacOS/Python
# coding: utf-8

import gflags
import sys
import time
import logging

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine, get_rawdb_conn

from guang_crawler.taobao_api import get_taobao_shops, get_rand_top
from guang_crawler.taobao_list_html import TaobaoListHtml, ShopOfflineException

gflags.DEFINE_integer('shopid', 0, "crawl shop id")
gflags.DEFINE_integer('limit', 0, "limit crawl shop count")
gflags.DEFINE_integer('interval', 0, "crawl interval between items")
gflags.DEFINE_string('where', "", "additional where sql, e.g. a=b and c=d")
gflags.DEFINE_string('sql', "", "custom sql, select id,url,crawl_status,name from shop")
gflags.DEFINE_boolean('all', False, "crawl all shops")
gflags.DEFINE_boolean('force', False, "is crawl offline shops?")
gflags.DEFINE_boolean('debug_parser', False, "debug html parser?")
gflags.DEFINE_boolean('dump', False, "dump html content?")

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

def resql(sql):
    if FLAGS.sql:
        return FLAGS.sql
    if FLAGS.limit:
        limit_sql = " limit %s" % FLAGS.limit
    else:
        limit_sql = ""
    if FLAGS.where:
        where_sql = " and %s " % FLAGS.where
    else:
        where_sql = " and 1 "
    return sql % (where_sql, limit_sql)

def check_all_shop():
    sql = resql("select id,url,crawl_status,name,type,nick from shop where (type=1 or type=2) and status=1 %s %s")
    check_shops(sql)

def check_shop():
    sql = "select id,url,crawl_status,name,type,nick from shop where id=%s" % FLAGS.shopid
    check_shops(sql)

def check_shops(sql):
    db = get_db_engine()

    last_time = 0
    shops = db.execute(sql)
    logger.info("checking total %s", shops.rowcount)
    failed = []
    for shop in shops:
        cur = time.time()*1000
        if cur - last_time < FLAGS.interval:
            time.sleep((FLAGS.interval-(cur-last_time))/1000.0)
        last_time = time.time()*1000
        check_one_shop(shop, failed)
    logger.info("Checked result, total %s failed %s", shops.rowcount, len(failed))
    for f in failed:
        logger.warn("%s %s", f['shopid'], f['err'])

def check_one_shop(shop, failed):
    shopid = shop[0]
    shop_url = shop[1]
    shop_nick = shop[5]

    shopinfo = get_taobao_shops(get_rand_top(), shop_nick)
    db = get_db_engine()
    try:
        tb = TaobaoListHtml(shopid, shop_url)
        tb.crawl(maxpage=1)
        page_len = tb.count
    except ShopOfflineException:
        page_len = 0
        if shopinfo.get('error', 0) == 560:
            logger.error("Shop %s url is offline! %s", shopid, shop_url)
            db.execute("update shop set status=2 where id=%s", shopid)
        else:
            logger.error("Shop %s url is error! %s --> %s", shopid, shop_url, shopinfo)

    compare_item_indb(db, page_len, shop_url, shopid)

def compare_item_indb(db, page_len, shop_url, shopid):
    db_items = db.execute("select count(id) from item where shop_id=%s and status=1", shopid)
    crawled_items = db.execute("select itemids from tb_shop_item where shopid=%s", shopid)
    items = []
    item_set = set()
    for crawl_item in crawled_items:
        items.extend(crawl_item[0].split(","))
    item_set.update(items)
    db_len = list(db_items)[0][0]
    crawl_db_len = len(item_set)
    if not page_len or abs(page_len - db_len) * 1.0 / page_len > 0.1:
        logger.warn("Items %s in db %s, in crawl %s -> %s, webpage %s, url %s", shopid, db_len, len(items),
                    crawl_db_len, page_len, shop_url)
    else:
        logger.info("Items %s in db %s, in crawl %s -> %s, webpage %s, url %s", shopid, db_len, len(items),
                    crawl_db_len, page_len, shop_url)

def check_shop_main():
    if FLAGS.all:
        check_all_shop()
    elif FLAGS.shopid > 0:
        check_shop()
    else:
        print 'Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS)

if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    check_shop_main()
