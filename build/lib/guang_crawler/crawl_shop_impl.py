# coding: utf-8

import gflags
import logging
from guang_crawler.taobao_api import get_taobao_shops, get_rand_top
import sys
import time
import traceback

from pygaga.helpers.dbutils import get_db_engine, get_rawdb_conn
from pygaga.helpers.statsd import Statsd

from guang_crawler.taobao_list_html import TaobaoListHtml, ShopOfflineException

try:
    from guang_crawler.mapreduce import SimpleMapReduce, identity
    has_multiprocessing = True
except:
    has_multiprocessing = False

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_boolean('dumpsql', False, "dump sql file")
gflags.DEFINE_boolean('updatevolume', True, "is update volume?")
gflags.DEFINE_string('dumpsqlfile', 'dump.sql', "dump sql file")

def dumpsql(sql):
    if not FLAGS.dumpsql:
        return
    f = open(FLAGS.dumpsqlfile, "a")
    f.write(sql)
    f.write(";\n")
    f.close()

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

def crawl_recent_shop():
    sql = resql("select distinct shop.id,shop.url,shop.crawl_status,shop.name,shop.type,shop.nick from shop left join tb_shop_item on shop.id=tb_shop_item.shopid where (tb_shop_item.created is null or tb_shop_item.created<DATE_SUB(now(),interval " + str(FLAGS.recenthour) + " hour)) and (shop.type=1 or shop.type=2) and shop.status=1 %s %s")
    crawl_shops(sql)

def crawl_pending_shop():
    sql = resql("select id,url,crawl_status,name,type,nick from shop where (type=1 or type=2) and status=1 and crawl_status=1 %s %s")
    crawl_shops(sql)

def crawl_all_shop():
    sql = resql("select id,url,crawl_status,name,type,nick from shop where (type=1 or type=2) and status=1 %s %s")
    crawl_shops(sql)

def crawl_shop():
    sql = "select id,url,crawl_status,name,type,nick from shop where id=%s" % FLAGS.shopid
    crawl_shops(sql)

def crawl_shops(sql):
    db = get_db_engine()

    last_time = 0
    dumpsql(sql)
    shops = db.execute(sql)
    logger.info("crawling total %s", shops.rowcount)
    failed = []
    for shop in shops:
        cur = time.time()*1000
        if cur - last_time < FLAGS.interval:
            time.sleep((FLAGS.interval-(cur-last_time))/1000.0)
        last_time = time.time()*1000
        crawl_one_shop({'shop':shop, 'is_commit':FLAGS.commit}, failed)
    logger.info("Crawled result, total %s failed %s", shops.rowcount, len(failed))
    for f in failed:
        logger.warn("%s %s", f['shopid'], f['err'])

def do_query(db, sql):
    dumpsql(sql)
    db.query(sql)

def crawl_one_shop(shop, failed):
    try:
        is_commit = shop['is_commit']
        shop_id = shop['shop'][0]
        shop_url = shop['shop'][1]
        shop_type = shop['shop'][4]
        shop_nick = shop['shop'][5]

        tb = TaobaoListHtml(shop_id, shop_url)
        tb.crawl()
        logger.debug("crawl result %s count %s total %s", tb.id, tb.count, len(tb.total_items))

        if is_commit:
            batch_size=100
            total_items = tb.total_items

            db = get_rawdb_conn()
            update_shop_items(batch_size, db, shop_id, total_items)
            update_taobao_volume(db, shop_id, shop_type, total_items)
            db.close()

            Statsd.increment('guang.crawl.shop_list_succ')
    except ShopOfflineException:
        #double check shop status by taobao api
        shopinfo = get_taobao_shops(get_rand_top(), shop_nick)
        if shopinfo.get('error', 0) == 560 and is_commit:
            db = get_rawdb_conn()
            do_query(db, "update shop set status=2 where id=%s" % shop_id)
            db.commit()
            db.close()
    except:
        Statsd.increment('guang.crawl.shop_list_failed')
        logger.error("crawl shop failed %s %s", shop_id, traceback.format_exc(), extra={'tags':['crawlShopException',]})
        failed.append({'shopid':shop_id, 'err':traceback.format_exc()})

def update_shop_items(batch_size, db, shop_id, total_items):
    # replace records in tb_crawl_shop_items
    logger.info("updating tb_crawl_shop_items %s", shop_id)
    do_query(db, "delete from tb_crawl_shop_items where shopid=%s" % shop_id)
    batch_count = (len(total_items) + batch_size - 1) / batch_size
    batch_items = [total_items[i::batch_count] for i in range(batch_count)]
    for bi in batch_items:
        logger.debug("inserting tb_crawl_shop_items %s", shop_id)
        insert_sql2 = "insert into tb_crawl_shop_items(shopid, iid, price, volume, title, status) values " + ",".join([
            "(%s, '%s', %s, %s, '%s', 0)" % (shop_id, item['iid'], item['price'], item['sales_amount'],
                                             item['desc'].replace("'", "''").replace('%', '%%').encode('utf8')) for item
            in bi])
        do_query(db, insert_sql2)

    # replace tb_shop_item
    logger.info("updating tb_shop_item %s", shop_id)
    do_query(db, "delete from tb_shop_item where shopid=%s" % shop_id)
    batch_count2 = (len(total_items) + batch_size * 10 - 1) / (batch_size * 10)
    batch_items2 = [total_items[i::batch_count2] for i in range(batch_count2)]
    for bi2 in batch_items2:
        logger.debug("inserting tb_shop_item %s", shop_id)
        insert_sql3 = "insert into tb_shop_item(shopid, itemids, status) values (%s, '%s', 0)" % (
            shop_id, ",".join([item['iid'] for item in bi2]))
        do_query(db, insert_sql3)
    db.commit()

def update_taobao_volume(db, shop_id, shop_type, total_items):
    # fetch current volumes & price
    logger.info("fetching current volumes %s", shop_id)
    do_query(db, "select num_id, volume, price from item where shop_id=%s" % shop_id)
    results = db.store_result()
    iid_volumes = {}
    for row in results.fetch_row(maxrows=0):
        iid_volumes[row[0]] = row[1]

    # update taobao volume, not tmall
    if FLAGS.updatevolume and shop_type == 1:
        db.autocommit(False)
        db.query("set autocommit=0;")
        # update volume
        logger.info("updating item volume %s", shop_id)
        for item in total_items:
            new_value = item['sales_amount']
            old_value = iid_volumes.get(item['iid'], 0) or 0
            diff_v = abs(new_value - old_value)
            if not iid_volumes.has_key(item['iid']):
                continue
                # 10% or 20 changed, update
            if new_value > 0 and new_value != old_value and (
                            old_value == 0 or diff_v > 20 or diff_v * 1.0 / old_value > 0.1):
                logger.debug("updating item %s %s %s -> %s", shop_id, item['iid'], old_value, new_value)
                do_query(db,
                         "update item set volume=%s where num_id=%s and shop_id=%s" % (new_value, item['iid'], shop_id))
                Statsd.increment('guang.crawl.volume_update_onlist')
        db.commit()

    # TODO: update tmall total volumes
    if FLAGS.updatevolume and shop_type == 2:
        pass

def crawl_shop_main():
    if FLAGS.pending:
        crawl_pending_shop()
    elif FLAGS.recent:
        crawl_recent_shop()
    elif FLAGS.all:
        crawl_all_shop()
    elif FLAGS.shopid > 0:
        crawl_shop()
    else:
        print 'Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS)
