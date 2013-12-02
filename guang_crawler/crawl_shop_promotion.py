#! /usr/bin/env python
# coding: utf-8

"""
    抓取店铺动态页面的促销商品信息
    1.店铺动态页：http://dodoz.taobao.com/dongtai.htm
    2.从促销页面获取userId,注意tmall店可能无促销信息
    3.组装url：http://shuo.taobao.com/feed/v_front_feeds.htm?_input_charset=utf-8&page=1&userId=59703832&vfeedTabId=115
    4.下载并获取“店铺促销中”商品
    5.根据商品链接获取num_iid
"""

import gflags
import logging
import traceback
import urllib2

from pygaga.helpers.utils import get_val, get_num_val
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
    crawler("select id, url, type from shop where status=1")

def crawl_shop(shop_id):
    crawler("select id, url, type from shop where id=%s" % shop_id)

def get_numiid(url, headers):
    req = urllib2.Request(url, headers=headers)
    resp = urllib2.urlopen(req)
    return resp.info().getheader('at_itemId')

def crawler(sql):
    db = get_db_engine()
    shops = list(db.execute(sql))

    # debug
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()

    for shop in shops:
        shop_id = shop[0]
        url = str(shop[1])
        type = shop[2]
        if url[-1] != '/':
            url += "/"
        try:
            shop_headers = {'Referer': url, 'User-Agent': DEFAULT_UA}
            dongtai_url = url + "dongtai.htm"
            dongtai_data = download(dongtai_url, shop_headers)
            if dongtai_data:
                dongtai_obj = parse_html(dongtai_data, encoding="gb18030")
                dongtai_title = dongtai_obj.xpath("//title/text()")[0].encode('utf-8')
                if '店铺动态' in dongtai_title:
                    microscope_data = dongtai_obj.xpath("//*[@name='microscope-data']/@content")
                    userId = get_val(str(microscope_data), "userId")

                    if userId:
                        dongtai_headers = {'Referer': dongtai_url, 'User-Agent': DEFAULT_UA}
                        promotion_url = "http://shuo.taobao.com/feed/v_front_feeds.htm?_input_charset=utf-8&page=1" \
                                        "&userId=%s&vfeedTabId=115" % userId
                        promotion_data = download(promotion_url, dongtai_headers)

                        if promotion_data:
                            promotion_obj = parse_html(promotion_data, encoding="gb18030")
                            i = 0
                            while i < 10:
                                feedInfo = promotion_obj.xpath("//div[@class='fd-item show-detail']//div[@class='fd-text']//span[@class='J_FeedInfo']/text()")[i].encode('utf-8')
                                if '店铺促销中' in feedInfo or '限时折扣' in feedInfo or '折扣限时' in feedInfo:
                                    #title = promotion_obj.xpath("//div[@class='fd-item show-detail']//div[@class='fd-container']//dt//a/text()")[i]
                                    link = promotion_obj.xpath("//div[@class='fd-item show-detail']//div[@class='fd-container']//dd//a[@class='fd-view-detail']/@href")[i]
                                    promotion_price = promotion_obj.xpath("//div[@class='fd-item show-detail']//div[@class='fd-container']//dd//span[@class='g_price']/strong/text()")[i]
                                    price = promotion_obj.xpath("//div[@class='fd-item show-detail']//div[@class='fd-container']//dd//span[@class='g_price g_price-original']/strong/text()")[i]
                                    promotion_time = promotion_obj.xpath(u"//div[@class='fd-item show-detail']//div[@class='fd-container']//dd[contains(text(),'起止日期')]/text()")[i]
                                    pt = promotion_time.encode('utf-8').replace("起止日期：","").split(" - ")
                                    start_time = pt[0].replace(".", "-")
                                    end_time = pt[1].replace(".", "-")
                                    if '2013' not in pt[1] or '2014' not in pt[1]:
                                        end_time = '2013-' + end_time

                                    if start_time > end_time:
                                        end_time = end_time.replace("2013", "2014")

                                    num_id = get_numiid(link, dongtai_headers)
                                    if num_id:
                                        sql = "select id from shop_promotion where shop_id=%s and num_id=%s" % (shop_id, num_id)
                                        re = list(db.execute(sql))
                                        if not re:
                                            db.execute("insert into shop_promotion (shop_id, num_id, price, "
                                                       "promotion_price, start_time, end_time, create_time, "
                                                       "last_update_time) values (%s,'%s',%s,%s,'%s','%s',now(),now())"
                                                       % (shop_id, num_id, price.replace(',', ''), promotion_price.replace(',', ''), start_time, end_time))
                                    else:
                                        logger.error("shop %s:%s crawler num_id failed", shop_id, url)

                                i += 1
                                logger.info("shop %s:%s crawler promotiom item num=%s", shop_id, url, i)

                        else:
                            logger.warning("shop %s:%s not promotion info", shop_id, url)
                    else:
                        logger.error("shop %s:%s crawler userId failed", shop_id, url)
                else:
                    logger.error("shop %s:%s not dongtai page", shop_id, url)
        except:
            logger.error("shop %s:%s crawler failed %s", shop_id, url, traceback.format_exc())


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    if FLAGS.shopid > 0:
        crawl_shop(FLAGS.shopid)
    elif FLAGS.all:
        crawl_all_shop()
