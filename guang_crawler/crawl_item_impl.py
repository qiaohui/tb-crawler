# coding: utf-8

import datetime
import gflags
import os
import logging
import redis
import re
import sys
import socket
import time
import traceback

from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.statsd import Statsd

from .taobao_html import TaobaoHtml

try:
    from .mapreduce import SimpleMapReduce, identity
    has_multiprocessing = True
except:
    has_multiprocessing = False

logger = logging.getLogger('CrawlLogger')

gflags.DEFINE_boolean('commit_html', True, "is commit to crawl_html")

FLAGS = gflags.FLAGS

def get_redis(host, port):
    return redis.Redis(host, port)

def resql(sql):
    if FLAGS.limit:
        limit_sql = " limit %s" % FLAGS.limit
    else:
        limit_sql = ""
    if FLAGS.where:
        where_sql = " and %s " % FLAGS.where
    else:
        where_sql = " and 1 "
    return sql % (where_sql, limit_sql)

def crawl_pending_items():
    if FLAGS.force:
        return crawl_items(resql("""select item.id,item.detail_url,item.num_id from item,shop
              where item.shop_id=shop.id and shop.type!=3
              and item.crawl_status=0 %s order by item.id desc %s"""))
    else:
        return crawl_items(resql("""select item.id,item.detail_url,item.num_id from item,shop
              where item.shop_id=shop.id and shop.type!=3
              and item.status=1 and shop.status=1
              and item.crawl_status=0 %s order by item.id desc %s"""))

def crawl_updated_items():
    # 销量或者价格不一样则更新
    if FLAGS.force:
        return crawl_items(resql("""select i.id,i.detail_url,i.num_id from item i
                left join tb_crawl_shop_items ti on ti.iid=i.num_id
                where (ti.volume != i.volume or ti.price != i.price)
                %s order by i.id desc %s
                """))
    else:
        if FLAGS.mod_id == 1:
            return crawl_items(resql("""select i.id,i.detail_url,i.num_id from item i
                    left join tb_crawl_shop_items ti on ti.iid=i.num_id
                    left join shop on i.shop_id=shop.id
                    where (ti.volume != i.volume or ti.price != i.price)
                    and i.status=1 and shop.type!=3 and shop.status=1
                    and mod(i.id, 2)=1
                    %s order by i.id desc %s
                    """))
        elif FLAGS.mod_id == 0:
            return crawl_items(resql("""select i.id,i.detail_url,i.num_id from item i
                    left join tb_crawl_shop_items ti on ti.iid=i.num_id
                    left join shop on i.shop_id=shop.id
                    where (ti.volume != i.volume or ti.price != i.price)
                    and i.status=1 and shop.type!=3 and shop.status=1
                    and mod(i.id, 2)=0
                    %s order by i.id desc %s
                    """))
        else:
            return crawl_items(resql("""select i.id,i.detail_url,i.num_id from item i
                    left join tb_crawl_shop_items ti on ti.iid=i.num_id
                    left join shop on i.shop_id=shop.id
                    where (ti.volume != i.volume or ti.price != i.price)
                    and i.status=1 and shop.type!=3 and shop.status=1
                    %s order by i.id desc %s
                    """))

def crawl_all_items():
    if FLAGS.force:
        return crawl_items(resql("""select item.id,item.detail_url,item.num_id from item
            left join shop on item.shop_id=shop.id where shop.type!=3
            %s order by item.id desc %s"""))
    else:
        return crawl_items(resql("""select item.id,item.detail_url,item.num_id from item
            left join shop on item.shop_id=shop.id where item.status=1 and shop.type!=3 and shop.status=1
            %s order by item.id desc %s"""))

def crawl_shop():
    if FLAGS.force:
        return crawl_items(resql("""select item.id,item.detail_url,item.num_id from item,shop
            where item.shop_id=shop.id and shop.type!=3
            and shop.id=""" + str(FLAGS.shopid) + """ %s oder by item.id desc %s"""))
    else:
        return crawl_items(resql("""select item.id,item.detail_url,item.num_id from item,shop
            where item.shop_id=shop.id and shop.type!=3 and item.status=1
            and shop.id=""" + str(FLAGS.shopid) + """ %s oder by item.id desc %s"""))

def crawl_num():
    db = get_db_engine()
    if FLAGS.force:
        return crawl_items("select item.id,item.detail_url,item.num_id from item,shop where item.shop_id=shop.id and shop.type!=3 and item.num_id=%s" % FLAGS.numid)
    else:
        return crawl_items("select item.id,item.detail_url,item.num_id from item,shop where item.shop_id=shop.id and shop.type!=3 and item.status=1 and item.num_id=%s" % FLAGS.numid)

def crawl_hotest():
    #查出bi-db1中所有的item_hotest表item_id数据，这个表应该是每小时更新一次
    #写入一个临时表temp_item_hotest,写入前先删除旧数据
    #联合查询item,temp_item_hotest表，进行抓取评论,最多抓取20页
    bi_db = get_db_engine(dbhost=FLAGS.bihost) 
    itemid_list = list(bi_db.execute("select item_id from item_hotest"))

    db = get_db_engine()
    db.execute("TRUNCATE table temp_item_hotest")
    logger.debug("TRUNCATE table temp_item_hotest")
    db.execute("insert into temp_item_hotest values (%s)", itemid_list)
    logger.debug("insert temp_item_hotest")
    if FLAGS.force:
        return crawl_items("select item.id,item.detail_url,item.num_id from item,temp_item_hotest where item.id=temp_item_hotest.item_id")
    else:
        return crawl_items("select item.id,item.detail_url,item.num_id from item,temp_item_hotest where item.status=1 and item.id=temp_item_hotest.item_id order by item.id desc")

def crawl_item():
    if FLAGS.force:
        return crawl_items("select item.id,item.detail_url,item.num_id from item,shop where item.shop_id=shop.id and shop.type!=3 and item.id=%s" % FLAGS.itemid)
    else:
        return crawl_items("select item.id,item.detail_url,item.num_id from item,shop where item.shop_id=shop.id and shop.type!=3 and item.status=1 and item.id=%s" % FLAGS.itemid)

def transform_args(iter):
    for i, item in enumerate(iter):
        yield ({'item':item, 'is_commit':FLAGS.commit, 'i':i, 'total':iter.rowcount,'max_comments':FLAGS.max_comments,
                'dbuser':FLAGS.dbuser, 'dbpasswd':FLAGS.dbpasswd, 'dbhost':FLAGS.dbhost, 'dbport':FLAGS.dbport, 'db':FLAGS.db, 'echosql':FLAGS.echosql})

def crawl_items(sql):
    db = get_db_engine()

    last_time = 0
    items = db.execute(sql)
    logger.info("crawling total %s", items.rowcount)
    if has_multiprocessing and FLAGS.parallel and False: # not parallel crawl detail pages
        mapper = SimpleMapReduce(crawl_item2, identity)
        results = mapper(transform_args(items))
        logger.info("crawl finished %s", len(results))
    else:
        for i, item in enumerate(items):
            cur = time.time()
            if cur - last_time < FLAGS.interval/1000.0:
                time.sleep(FLAGS.interval/1000.0-(cur-last_time))
            last_time = time.time()
            crawl_item2({'item':item, 'is_commit':FLAGS.commit, 'i':i, 'total':items.rowcount, 'max_comments':FLAGS.max_comments})

def crawl_item2(kwargs):
    item = kwargs['item']
    is_commit = kwargs['is_commit']
    is_success = False
    item_id = item[0]
    num_id = item[2]
    crawl_result = ((item_id, (0,0,0,0,0,0.0,0)),)

    tb = TaobaoHtml(item_id, num_id, max_comments=kwargs['max_comments'])

    db = None
    if is_commit:
        db = get_db_engine()

    try:
        logger.info("progress %s/%s id %s iid %s", kwargs['i'], kwargs['total'], item_id, num_id)
        tb.crawl()
        if tb.is_offline and is_commit:
            db.execute("update item set status=2, modified=now() where id=%s" % item_id)
        if tb.detailDiv and not tb.is_offline:
            tb.crawl_price()

            if is_commit:
                # update item_other
                db.execute("replace into item_other (item_id,num_id,cid,confirmVolume,collection,browse,stock,postage) values (%s,%s,%s,%s,%s,%s,%s,%s)",
                           item_id, num_id, tb.cid, tb.confirmVolume, tb.collection, tb.browse, tb.stock, tb.postage)

                # check old price and volume
                pv = list(db.execute("select price, volume from item where id=%s", item_id))
                price = pv[0][0]
                volume = pv[0][1]
                if tb.price != price and tb.price > 0.001:
                    is_price_update = True
                else:
                    is_price_update = False
                if tb.volume > 0 and tb.volume != volume:
                    is_volume_update = True
                else:
                    is_volume_update = False

                if is_price_update:
                    db.execute("insert into price_update_track (item_id,time) values (%s,now()) on duplicate key update time=now()" % item_id)
                    if is_volume_update:
                        db.execute("update item set price=%s, volume=%s where id=%s", tb.price, tb.volume, item_id)
                    else:
                        db.execute("update item set price=%s where id=%s", tb.price, item_id)
                elif is_volume_update:
                    db.execute("update item set volume=%s where id=%s", tb.volume, item_id)
                if is_price_update:
                    Statsd.increment("taobao.crawl.price_update")
                if is_volume_update:
                    Statsd.increment("taobao.crawl.volume_update")

            if FLAGS.update_main:
                tb.crawl_desc()

                if len(tb.thumbImages) > 0 and is_commit and FLAGS.commit_html:
                    db.execute("delete from crawl_html where item_id=%s" % item_id)
                    db.execute("insert into crawl_html (item_id,desc_url,promo_url,html,desc_content,promo_content,result,reason) values (%s, %s, %s, %s, %s, %s, %s, %s)", item_id, tb.descUrl, tb.promoteUrl, tb.data.decode('gb18030').encode('utf8'), tb.descContent.decode('gb18030').encode('utf8'), tb.promoteContent.decode('gb18030').encode('utf8'), 1, "")
                    db.execute("update item set crawl_status=1 where id=%s" % item_id)
                    Statsd.increment("taobao.crawl.html_update")

            ############### processing comments ###########
            if FLAGS.update_comments:
                rediscli = get_redis(FLAGS.redishost, FLAGS.redisport)
                key = "guang:rate:%s" % item_id
                l = rediscli.llen(key)
                tb.crawl_rate()
                logger.info("replace comments %s %s -> %s", item_id, l, len(tb.comments))
                #rediscli.lrange(key, 0, l)
                rediscli.delete(key)
                for c in tb.comments:
                    rediscli.rpush(key, c.SerializeToString())
                    # if limit size
                    #p = rediscli.pipeline()
                    #p.rpush(key, c.SerializeToString())
                    #p.ltrim(0, 99)
                    #p.execute()
                Statsd.increment("taobao.crawl.comments_update")
                Statsd.update_stats("taobao.crawl.comments_update_total", len(tb.comments))

            is_success = True
            crawl_result = ((item_id, (len(tb.data),len(tb.promoteContent),len(tb.descContent),len(tb.thumbImages),len(tb.buyButton),tb.price,len(tb.comments))),)
        else:
            logger.warn("crawl %s failed, no detail content or is_offline=%s", item_id, tb.is_offline)
            crawl_result = ((item_id, (len(tb.data),0,0,0,0,0.0,0)),)
    except:
        logger.error("crawling %s unknown exception %s", item_id, traceback.format_exc(), extra={'tags':['crawlItemException',]})
    logger.info("crawling %s result %s - %s", item_id, is_success, crawl_result)
    if is_success:
        Statsd.increment("taobao.crawl.itemhtml.succ")
    else:
        Statsd.increment("taobao.crawl.itemhtml.failed")
    return crawl_result

def crawl_item_main():
    if FLAGS.pending:
        crawl_pending_items()
    elif FLAGS.all:
        crawl_all_items()
    elif FLAGS.changed:
        crawl_updated_items()
    elif FLAGS.itemid > 0:
        crawl_item()
    elif FLAGS.numid > 0:
        crawl_num()
    elif FLAGS.shopid > 0:
        crawl_shop()
    elif FLAGS.hotest:
        crawl_hotest()
    else:
        print 'Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS)
