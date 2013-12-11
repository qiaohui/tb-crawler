#! /usr/bin/env python
# coding: utf-8

"""
    抓取店铺优惠信息，类似“全场满150元包快递”
    url：http://tds.alicdn.com/json/promotionNew.htm?user_id=33197951&item_id=36282084977&mjs=1&meal=&v=1&callback=DT.mods.SKU.Promotion.getDefaultData
"""

import gflags
import logging
import traceback
import datetime
import time
from pygaga.simplejson import loads

from pygaga.helpers.utils import get_val
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download, parse_html

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_boolean('all', False, "update all shop")
gflags.DEFINE_integer('shopid', 0, "update shop id")
gflags.DEFINE_boolean('force', False, "is update offline shops?")
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"

def crawl_all_shop():
    crawler("select s.id,s.type,i.id,i.detail_url from item i, shop s where i.shop_id=s.id and i.status=1 and i.crawl_status=2 and s.status=1 group by i.shop_id")

def crawl_shop(shop_id):
    crawler("select s.id,s.type,i.id,i.detail_url from item i, shop s where i.shop_id=s.id and i.status=1 and i.crawl_status=2 and s.status=1 and s.id=%s group by i.shop_id" % shop_id)

def get_item_htm(id, url, db):
    sql = "select html,last_modified from crawl_html where item_id=%s" % id
    item_htm = list(db.execute(sql))
    last_modified = item_htm[0][1]
    now = datetime.datetime.now()
    days = now - last_modified
    if days > datetime.timedelta(days=7):
        item_headers = {'Referer': url,'User-Agent': DEFAULT_UA}
        item_htm = download(url, item_headers)
        db.execute("update crawl_html set html=%s,last_modified=now() where item_id=%s", item_htm.decode('gb18030').encode('utf8'), id) 
        return item_htm
    else:
        return item_htm[0][0]

def crawler(sql):
    db = get_db_engine()
    items = list(db.execute(sql))

    # debug
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()

    for item in items:
        shop_id = item[0]
        shop_type = item[1]
        item_id = item[2]
        url = item[3]

        try:
            htm = get_item_htm(item_id, url, db)
            if shop_type == 1:
                htm_obj = parse_html(htm, encoding='gb18030')
                discount_url = htm_obj.xpath("//div[@id='promote']/@data-default")
                if discount_url and len(discount_url) > 0:
                    item_headers = {'Referer': url, 'User-Agent': DEFAULT_UA}
                    disc_content = download(discount_url[0], item_headers)
                    if disc_content.strip():
                        disc_obj = parse_html(disc_content, encoding='gb18030')
                        content = disc_obj.xpath("//div[@id='J_MjsData']/h3/text()")[0].strip()
                        dates = disc_obj.xpath("//div[@id='J_MjsData']/h3/span[@class='tb-indate']/text()")[0].strip()
                        st = dates.encode('utf-8').replace("--","—").split("—")
                        start_time = datetime.datetime.strptime(st[0].strip().replace('年','-').replace("月","-").replace("日",""),'%Y-%m-%d')
                        end_time = datetime.datetime.strptime(st[1].strip().replace('年','-').replace("月","-").replace("日",""),'%Y-%m-%d')

                        db.execute("replace into shop_discount (shop_id,content,start_time,end_time,discount_url,create_time,last_update_time) values (%s,%s,%s,%s,%s,now(),now())",
                                   shop_id, content.encode('utf-8'), start_time, end_time, discount_url[0])
                        logger.info("taobao shop %s get discount success", shop_id)
                    else:
                        logger.warning("taobao shop %s:%s not discount.", shop_id, url)
                else:
                    logger.warning("taobao shop %s:%s not discount.", shop_id, url)
            elif shop_type == 2:
                d_url = get_val(htm, "initApi")
                if d_url:
                    item_headers = {'Referer': url, 'User-Agent': DEFAULT_UA}
                    disc_content = download(d_url, item_headers)
                    cjson = loads(disc_content.decode('gb18030').encode('utf8'))
                    shop_prom = cjson['defaultModel']['itemPriceResultDO']['tmallShopProm']
                    if shop_prom:
                        st = int(shop_prom['startTime'])/1000
                        et = int(shop_prom['endTime'])/1000
                        start_time = time.strftime("%Y-%m-%d", time.localtime(st))
                        end_time = time.strftime("%Y-%m-%d", time.localtime(et))
                        content = shop_prom['promPlan'][0]['msg']
                        db.execute("replace into shop_discount (shop_id,content,start_time,end_time,discount_url,create_time,last_update_time) values (%s,%s,%s,%s,%s,now(),now())",
                            shop_id, content.encode('utf-8'), start_time, end_time, d_url)
                        logger.info("tmall shop %s get discount success", shop_id)
                    else:
                        logger.warning("taobao shop %s:%s not discount.", shop_id, url)
        except:
            logger.error("shop %s:%s xpath failed:%s", shop_id, url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    if FLAGS.shopid > 0:
        crawl_shop(FLAGS.shopid)
    elif FLAGS.all:
        crawl_all_shop()
