# coding: utf-8

import gflags
import os
import logging
import re
import sys
import signal
import traceback

from pygaga.helpers.mapreduce_multiprocessing import SimpleMapReduce, identity
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import parse_html
from pygaga.helpers.statsd import Statsd

from guang_crawler.image_crawler import ItemCrawler

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

IMAGESTYLE_RE = re.compile("^.*url\(([^\)]+)\)$", re.M|re.S)

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
        #return crawl_items(resql("select item_id,num_id from crawl_html,item where item.id=crawl_html.item_id and is_image_crawled=0 %s order by item_id desc %s"))
        return crawl_items(resql("select id,num_id from item where crawl_status=1 %s order by id desc %s"))
    else:
        #return crawl_items(resql("select item_id,num_id from crawl_html,item,shop where item.id=crawl_html.item_id and is_image_crawled=0 and item.status=1 and item.shop_id=shop.id and shop.status=1 %s order by item_id desc %s"))
        return crawl_items(resql("select item.id,num_id from item,shop where item.crawl_status=1 and item.shop_id=shop.id and shop.status=1 and item.status=1 %s order by id desc %s"))

def crawl_all_items():
    if FLAGS.force:
        return crawl_items(resql("select item_id,num_id from crawl_html,item where item.id=crawl_html.item_id %s order by item_id desc %s"))
    else:
        return crawl_items(resql("select item_id,num_id from crawl_html,item,shop where item.id=crawl_html.item_id and item.status=1 and item.shop_id=shop.id and shop.status=1 %s order by item_id desc %s"))

def crawl_one_item(item_id):
    if FLAGS.force:
        return crawl_items("select item_id,num_id from crawl_html,item where item.id=crawl_html.item_id and item.id=%s" % item_id)
    else:
        return crawl_items("select item_id,num_id from crawl_html,item where item.id=crawl_html.item_id and item.status=1 and item.id=%s" % item_id)

def crawl_one_num(num_id):
    if FLAGS.force:
        return crawl_items("select item_id,num_id from crawl_html,item where item.id=crawl_html.item_id and item.num_id=%s" % num_id)
    else:
        return crawl_items("select item_id,num_id from crawl_html,item where item.id=crawl_html.item_id and item.status=1 and item.num_id=%s" % num_id)

def transform_args(iter):
    for i in iter:
        yield ({'item':i, 'is_commit':FLAGS.commit, 'crawl_path':FLAGS.crawl_path,
            'server_path':FLAGS.path, 'is_remove':FLAGS.removetmp, 'org_server_path':FLAGS.org_path,
            'dbuser':FLAGS.dbuser, 'dbpasswd':FLAGS.dbpasswd, 'dbhost':FLAGS.dbhost, 'dbport':FLAGS.dbport,
            'db':FLAGS.db, 'echosql':FLAGS.echosql,
            'statshost':FLAGS.statshost, 'statsport':FLAGS.statsport})

def crawl_items(sql):
    db = get_db_engine()

    items = db.execute(sql)
    logger.info("crawling image total %s", items.rowcount)
    if not items.rowcount:
        return
    if FLAGS.parallel:
        mapper = SimpleMapReduce(crawl_item2, identity)
        results = mapper(transform_args(items))
        logger.info("crawl finished %s", len(results))
    else:
        for item in items:
            crawl_item2({'item':item, 'is_commit':FLAGS.commit, 'crawl_path':FLAGS.crawl_path,
            'server_path':FLAGS.path, 'is_remove':FLAGS.removetmp, 'org_server_path':FLAGS.org_path,
            'dbuser':FLAGS.dbuser, 'dbpasswd':FLAGS.dbpasswd, 'dbhost':FLAGS.dbhost, 'dbport':FLAGS.dbport,
            'db':FLAGS.db, 'echosql':FLAGS.echosql,
            'statshost':FLAGS.statshost, 'statsport':FLAGS.statsport})

def crawl_item2(kwargs):
    #signal.signal(signal.SIGINT, signal.SIG_IGN)
    item = kwargs['item']
    is_commit = kwargs['is_commit']
    crawl_path = kwargs['crawl_path']
    server_path = kwargs['server_path']
    org_server_path = kwargs['org_server_path']
    is_remove = kwargs['is_remove']

    item_id = item[0]
    num_id = item[1]
    is_success = False
    crawl_result = ((item_id, {'suc1': 0, 'count1': 0, 'suc': 0, 'count': 0}),)
    try:
        conn = get_db_engine(**kwargs).connect()
        try:
            items = conn.execute("select html, desc_content from crawl_html where crawl_html.item_id=%s;" % item_id)
            result = list(items)
            if len(result) == 1:
                html = result[0][0]
                desc_content = result[0][1] 
                html_obj = parse_html(html)
                thumbImages = html_obj.xpath("//ul[@id='J_UlThumb']//img/@src")
                if len(thumbImages) == 0:
                    thumbImages = [IMAGESTYLE_RE.subn(r'\g<1>', x)[0] for x in html_obj.xpath("//ul[@id='J_UlThumb']//li/@style")]
                    # taobao @src to @data-src
                    if not len(thumbImages):
                        thumbImages = html_obj.xpath("//ul[@id='J_UlThumb']//img/@data-src")

                if len(thumbImages) == 0:
                    logger.error("crawl item %s %s not found thumb images html size %s", item_id, num_id, len(html), extra={'tags':['crawl_thumb_empty',]})
                    return crawl_result

                r = re.compile("(var desc='|)(.*)(\\\\|';)", re.M|re.S)
                tr = re.compile("(.*)_\d+x\d+\.jpg$")
                tr_new = re.compile("(.+\.(jpg|png|gif))[^$]*.jpg$")
                desc_thumbs = desc_table_thumbs = lazy_desc_thumbs = []
                if desc_content:
                    desc_html = r.subn(r'\2', desc_content)[0]
                    desc_html_obj = parse_html(desc_html)
                    if desc_html_obj is not None:
                        desc_table_thumbs = desc_html_obj.xpath("//table/@background")
                        desc_thumbs = desc_html_obj.xpath("//*[not(@href)]/img[not(@data-ks-lazyload)]/@src")
                        lazy_desc_thumbs = desc_html_obj.xpath("//*[not(@href)]/img/@data-ks-lazyload")
                else:
                    logger.warn("crawl item %s %s desc content is empty!", item_id, num_id, extra={'tags':['crawl_nodesc',]})

                images = []
                pos = 1
                for url in thumbImages:
                    ori_url = None
                    if tr.match(url):
                        ori_url = tr.sub(r'\1', url)
                    else:
                        if tr_new.match(url):
                            ori_url = tr_new.sub(r'\1', url)
                        else:
                            logger.error("crawl item %s %s thumb image urls can not be parsed!", item_id, num_id, extra={'tags':['crawl_exception',]})

                    images.append((ori_url, pos, 1))
                    pos += 1
                for url in desc_table_thumbs:
                    images.append((url, pos, 2))
                    pos += 1
                for url in desc_thumbs:
                    if "js/ckeditor" not in url:
                        images.append((url, pos, 2))
                        pos += 1
                for url in lazy_desc_thumbs:
                    if "js/ckeditor" not in url:
                        images.append((url, pos, 3))
                        pos += 1

                logger.debug("crawling %s %s %s", item_id, num_id, images)
                item_crawler = ItemCrawler(item_id, num_id, crawl_path, server_path, org_server_path, kwargs['statshost'], kwargs['statsport'])
                item_crawler.crawl(images, ((710,10000),), is_commit, conn, is_remove)
                is_success = item_crawler.success
                crawl_result = ((item_id, item_crawler.summary),)
        except Exception, e:
            logger.error("crawl item %s %s got exception %s", item_id, num_id, traceback.format_exc(), extra={'tags':['crawl_exception',]})
        finally:
            conn.close()
        Statsd.update_stats("guang.crawl.downimgcount", crawl_result[0][1]['suc1'] + crawl_result[0][1]['suc'],
            host = kwargs['statshost'], port = kwargs['statsport'])
        if is_success:
            logger.info("crawl item %s %s success %s", item_id, num_id, crawl_result)
            Statsd.increment('guang.crawl.itemimg.succ', host = kwargs['statshost'], port = kwargs['statsport'])
        else:
            logger.warn("crawl item %s %s failed %s", item_id, num_id, crawl_result, extra={'tags':['crawl_failed',]})
            Statsd.increment('guang.crawl.itemimg.failed', host = kwargs['statshost'], port = kwargs['statsport'])
    except KeyboardInterrupt:
        raise
    except Exception:
        logger.error("crawl item %s %s got exception1 %s", item_id, num_id, traceback.format_exc(), extra={'tags':['crawl_exception',]})
    return crawl_result

def crawl_image_main():
    if FLAGS.pending:
        crawl_pending_items()
    elif FLAGS.all:
        crawl_all_items()
    elif FLAGS.itemid > 0:
        crawl_one_item(FLAGS.itemid)
    elif FLAGS.numid > 0:
        crawl_one_num(FLAGS.numid)
    else:
        print 'Usage: %s ARGS\\n%s' % (sys.argv[0], FLAGS)
