#!/usr/bin/env python
# coding: utf-8

#import MySQLdb
import gflags
import os
import logging
import re
import sys
import signal
import socket
import simplejson
import time
import traceback
import urllib
import urllib2
from urllib2 import HTTPError, URLError
from lxml import etree
from lxml.html import soupparser

from pygaga.helpers.logger import log_init
from pygaga.helpers.cachedns_urllib import custom_dns_opener
from pygaga.helpers.dbutils import get_db_engine

try:
    from pygaga.helpers.mapreduce_multiprocessing import SimpleMapReduce, identity
    has_multiprocessing = True
except:
    has_multiprocessing = False

logger = logging.getLogger('MeiliCrawlLogger')

FLAGS = gflags.FLAGS
gflags.DEFINE_integer('itemid', 0, "start crawl id")
gflags.DEFINE_integer('group', 0, "define group*1000000 -> (group+1)*1000000")
gflags.DEFINE_integer('start', 2217, "start crawl id")
gflags.DEFINE_integer('end', 110538380, "end crawl id")
gflags.DEFINE_integer('interval', 0, "crawl interval between items")
gflags.DEFINE_boolean('commit', True, "is commit data into database?")

DEFAULT_UA="Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
urllib2.install_opener(custom_dns_opener())

"""
CREATE TABLE `crawl_html` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `item_id` int(11) unsigned NOT NULL,
  `html` longtext,
  `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `item_id` (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

headers = { 'Referer' : "http://www.meilishuo.com", 'User-Agent' : DEFAULT_UA }
def crawl_all():
    login_params = {'emailaddress':'songemma@126.com',
    'password':'12345678',
    'type':'undefined',
    'wbid':'0',
    'savestat':'true'
#    'checkcode':'',
    }
    req = urllib2.Request('http://www.meilishuo.com/users/ajax_logon?frm=undefined', urllib.urlencode(login_params), headers)
    handle = urllib2.urlopen(req)
    logger.info("logged result %s", handle.read())

    if FLAGS.itemid:
        crawl_item(FLAGS.itemid)
    else:
        if FLAGS.group:
            start = FLAGS.group*1000000
            end = (FLAGS.group+1)*1000000
        else:
            start = FLAGS.start
            end = FLAGS.end
        db = get_db_engine()
        for item_id in xrange(start, end, 1):
            try:
                results = db.execute("select item_id from crawl_html where item_id=%s" % item_id)
                if results.rowcount > 0:
                    continue
            except:
                db = get_db_engine()
            crawl_item(item_id)

def get_obj(html, expr):
    tmp = html.xpath(expr)
    if len(tmp) > 0:
        return tmp[0]
    return None

def crawl_item(item_id):
    try:
        url = "http://www.meilishuo.com/share/%s" % item_id
        data = crawl_page(item_id, url, headers)
        if not data:
            return
        try:
            html_obj = etree.XML(data)
        except:
            try:
                html_obj = soupparser.fromstring(data.decode('utf8'))
            except:
                try:
                    html_obj = etree.HTML(data)
                except:
                    logger.warn("crawling %s len %s parse failed %s", item_id, len(data), traceback.format_exc(), extra={'tags':['crawlItemParseException',]})

        #saved_data = etree.tostring(html_obj.xpath("//div[@id='main']/div/div/div")[0])
        detail_path = html_obj.xpath("//div[@id='main']/div/div/div")
        if not detail_path:
            logger.info("err parse %s len %s", item_id, len(data))
            return
        detail_obj = detail_path[0]

        results = {}
        results['user_url'] = get_obj(detail_obj, "div/dl/dt/a/@href")
        results['user_name'] = get_obj(detail_obj, "div/dl/dd[1]/a/text()")
        results['obj_date'] = get_obj(detail_obj, "div/dl/dd/span/text()")

        results['obj_url'] = get_obj(detail_obj, "div/div/div/p[1]/a/@href")
        results['obj_title'] = get_obj(detail_obj, "div/div/div/p[1]/a/text()")
        results['obj_img'] = get_obj(detail_obj, "div/div/a/img/@src")
        results['obj_fav_count'] = get_obj(detail_obj, "div/div/div/p[2]/a/b/text()")
        results['obj_org_img'] = get_obj(detail_obj, "div/div[@class='original_pic_ioc']/a/@href")
        results['obj_comment_count'] = get_obj(detail_obj, "div/div/div/a/b/text()")
        results['obj_price'] = get_obj(detail_obj, "div/div/div/div/p/text()")

        results['group_title'] = get_obj(detail_obj, "div/dl/dd[1]/a/text()")
        results['group_url'] = get_obj(detail_obj, "div/dl/dd[1]/a/@href")
        results['group_desc'] = get_obj(detail_obj, "div/dl/dd[1]/text()")

        logger.debug("results %s", results)
        #import pdb; pdb.set_trace()

        db = get_db_engine()
        db.execute("delete from crawl_html where item_id=%s" % item_id)
        db.execute("insert into crawl_html (item_id,html) values (%s, %s)", item_id, simplejson.dumps(results))
        logger.info("crawled %s len %s", url, len(data))
    except KeyboardInterrupt:
        raise
    except:
        logger.warn("crawl failed %s exception %s", url, traceback.format_exc())

def crawl_page(item_id, url, headers):
    logger.debug("Crawling %s", url)
    data = ""
    try:
        req = urllib2.Request(url, headers=headers)
        u = urllib2.urlopen(req)
        data = u.read()
        u.close()
    except ValueError, e:
        logger.info("download %s:%s url value error %s", item_id, url, e.message)
    except HTTPError, e1:
        if e1.code != 404:
            logger.info("download %s:%s failed http code: %s", item_id, url, e1.code)
    except URLError, e2:
        logger.info("download %s:%s failed url error: %s", item_id, url, e2.reason)
    except socket.timeout:
        logger.info("download %s:%s failed socket timeout", item_id, url)
    return data

if __name__ == "__main__":
    log_init("MeiliCrawlLogger")

    crawl_all()

