#!/usr/bin/env python
# coding: utf-8

import gflags
import sys
import logging
import traceback
from guang_crawler.taobao_api import get_taobao_shops, get_rand_top

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

from guang_crawler.taobao_list_html import TaobaoListHtml

gflags.DEFINE_integer('shopid', 0, "crawl shop id")
gflags.DEFINE_integer('interval', 0, "crawl interval between items")
gflags.DEFINE_string('where', "1", "additional where sql, e.g. a=b and c=d")
gflags.DEFINE_string('sql', "", "custom sql, select id,url,crawl_status,name from shop")
gflags.DEFINE_boolean('all', False, "crawl all shops")
gflags.DEFINE_boolean('force', False, "is crawl offline shops?")
gflags.DEFINE_boolean('debug_parser', False, "debug html parser?")
gflags.DEFINE_boolean('dump', False, "dump html content?")

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

def update_all_shop():
    sql = """select id,url,level,nick,sid,cid,taobao_created,taobao_modified,taobao_title,item_score,service_score,delivery_score
            from shop where status=1 and type<=2 and %s""" % FLAGS.where
    update_shop_level(sql)

def update_shop():
    sql = "select id,url,level,nick,sid,cid,taobao_created,taobao_modified,taobao_title,item_score,service_score," \
          "delivery_score from shop where id = %s" % FLAGS.shopid
    update_shop_level(sql)

def update_shop_level(sql):
    db = get_db_engine()
    shops = db.execute(sql)
    failed = []
    for shop in shops:
        process_shop(db, shop, failed)
    results = "Update shop's level, Checked result, total %s failed %s, detailed %s" % (shops.rowcount, len(failed), ",".join(map(str, failed)))
    if len(failed):
        logger.warn(results)
    else:
        logger.info(results)

def process_shop(db, shop, failed):
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()
    id,url,level,nick,sid,cid,taobao_created,taobao_modified,taobao_title,item_score,service_score,delivery_score = shop
    try:
        shopinfo = get_taobao_shops(get_rand_top(), nick)
        if shopinfo.get("error", 0) == 560:
            logger.warn("Shop nick maybe error! %s", id)
        new_shop = {}
        if shopinfo.has_key('shop'):
            new_shop['sid'] = shopinfo['shop']['sid']
            new_shop['cid'] = shopinfo['shop']['cid']
            new_shop['delivery_score'] = int(float(shopinfo['shop']['shop_score']['delivery_score']) * 10)
            new_shop['item_score'] = int(float(shopinfo['shop']['shop_score']['item_score']) * 10)
            new_shop['service_score'] = int(float(shopinfo['shop']['shop_score']['service_score']) * 10)

            new_shop['taobao_created'] = shopinfo['shop']['created']
            new_shop['taobao_modified'] = shopinfo['shop']['modified']
            new_shop['taobao_title'] = shopinfo['shop']['title']

        tb = TaobaoListHtml(id, url)
        tb.crawl(maxpage=1)
        if url.startswith('http://shop'):
            db.execute('update shop set nick_url="%s" where id=%s', tb.nick_url[0], id)
            logger.debug("nick url is %s", tb.nick_url[0])
        new_shop['level'] = tb.get_level()

        update_fields = []
        for key in new_shop:
            old_val = locals()[key]
            if new_shop[key] != old_val:
                update_fields.append((key, new_shop[key], old_val))
        if update_fields:
            update_sql = "update shop set %s where id=%s" % (",".join([get_set_sql(f) for f in update_fields]), id)
            logger.debug(update_sql)
            db.execute(update_sql)
    except KeyboardInterrupt:
        raise
    except:
        logger.warn("update shop(id=%s) level hash unknown exception %s", id, traceback.format_exc())
        failed.append(traceback.format_exc())
        return None

def get_set_sql(fields):
    key, new_val, old_val = fields
    if type(new_val) is unicode:
        new_val = new_val.encode('utf-8')
    if type(new_val) in (int, float, long):
        return "%s=%s" % (key, new_val)
    if type(new_val) is str:
        return "%s='%s'" % (key, new_val.replace("'", "''").replace('%','%%'))
    raise Exception("not support field type %s" % type(new_val))

def update_shop_level_main():
    if FLAGS.all:
        update_all_shop()
    elif FLAGS.shopid > 0:
        update_shop()
    else:
        print 'Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS)

if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    update_shop_level_main()
