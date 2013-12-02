#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
import re
import datetime
import time
import random
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import parse_html, download

from guang_crawler.taobao_shop_extend import ShopExtendInfo

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS

gflags.DEFINE_string('shop_ids', '', "update shop ids")
gflags.DEFINE_integer('interval', 0, "crawl interval between items")
gflags.DEFINE_boolean('force', False, "is update offline shops?")
gflags.DEFINE_boolean('debug_parser', False, 'is debug?')

#在Item页查找评价页url
url_reg = re.compile(r'\"(\s*http://rate.taobao.com/user-rate-[^\"]+)\"')
#去除所在地区的前缀
format_location_reg = re.compile(r'\s*所在地区：\s*[\r|\n]*\s*')
#去除好评率的前缀
format_good_item_rate_reg = re.compile(r'\s*好评率：\s*[\r|\n]*\s*')
#去除创店时间的前缀
format_open_at_reg = re.compile(r'\s*创店时间：\s*[\r|\n]*\s*')
#获取收藏人数结果
favorite_num_reg = re.compile(r'\":(\d+)')
#用于取出脚本和样式
#script_and_style = re.compile('(<\s*script[^>]*>[^<]*<\s*/\s*script\s*>)|(<\s*style[^>]*>[^<]*<\s*/\s*style\s*>)', re.I)


def download_with_referer(url, referer):
    """抓取店铺扩展信息时 强制要求加refer 如果不需要加 则refer赋值为None"""
    if referer:
        headers = {
            'Referer': referer,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
            'User-Agent': "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
        }
    else:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
            'User-Agent': "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
        }

    return download(url, headers)


def get_compare_rate(html_obj, keyword):
    """根据html的解析器和对应关键字 查找并返回淘宝天猫三项评分和比较"""
    rate_value = float(html_obj.xpath(u"//div[@class='item-scrib']/span[@class='title'][contains(text(),'" \
        + keyword + u"')]/following-sibling::em[1]/text()")[0].strip(u" \t\n\r"))
    rate_compare_sign = html_obj.xpath(u"//div[@class='item-scrib']/span[@class='title'][contains(text(),'" \
        + keyword + u"')]/following-sibling::em[2]/strong/@class")[0]

    rate_compare = 0
    if rate_compare_sign.find(u"normal") >= 0:
        rate_compare = 0
    else:
        rate_compare_value = float(html_obj.xpath(u"//div[@class='item-scrib']/span[@class='title'][contains(text(),'" \
            + keyword + u"')]/following-sibling::em[2]/strong/text()")[0].strip(u" \t\n\r%"))

        if rate_compare_sign.find(u"over") >= 0:
            rate_compare = rate_compare_value
        else:
            rate_compare = -rate_compare_value

    return [rate_value, rate_compare]


def get_taobao_shop_favorite_count(the_shop, shop_html_obj, urls):
    """获取淘宝店被收藏数目"""
    try:
        favorite_count_success = False

        favorite_param = shop_html_obj.xpath(u"//div[@class='item collect-num']/span[contains(@data-info,'SCCP')]/@data-info")
        if favorite_param:
            the_param = favorite_param[0].split('&')
            favorite_url = "%s?callback=jsonp%d&t=%s&keys=%s" % (
                the_param[1].split('=')[1], random.randint(1000, 9999), str(int(time.time() * 1000)),
                the_param[0].split('=')[1]
            )
            urls['favorite_url'] = favorite_url
            favorite_html = download_with_referer(favorite_url, urls['shop_rate_url'])
            if favorite_html:
                logger.debug("download shop favorite html. shop_id: %d, url: %s. html length: %d." % (
                    the_shop.get_shop_id(), favorite_url, len(favorite_html))
                )
                the_shop.favorited_user_count = int(favorite_num_reg.search(favorite_html).group(1))
                favorite_count_success = True
            else:
                logger.error(
                    "download shop favorite html error. shop_id: %d, url: %s." % (the_shop.get_shop_id(), favorite_url)
                )

        if not favorite_count_success:
            logger.debug("use pattern left edition to get favorite data ")
            favorite_param = shop_html_obj.xpath(u"//li[@id='J_SCollCount'][@data-info]/@data-info")
            if favorite_param:
                the_param = favorite_param[0].split('&')
                favorite_url = "%s?t=%s&keys=%s&callback=setShopStat" % (
                    the_param[1].split('=')[1], str(int(time.time() * 1000)),
                    the_param[0].split('=')[1]
                )
                favorite_html = download_with_referer(favorite_url, urls['shop_rate_url'])
                if favorite_html:
                    the_shop.favorited_user_count = int(favorite_num_reg.search(favorite_html).group(1))
                    favorite_count_success = True

        if not favorite_count_success:
            logger.debug("use pattern for old edition to get favorite data ")

            shop_description_url = shop_html_obj.xpath(u"//a[@title='店铺介绍']/@href")
            if shop_description_url:
                shop_description_html = download_with_referer(shop_description_url[0], urls['shop_rate_url'])
                if shop_description_html:
                    shop_description_html_obj = parse_html(shop_description_html, 'gbk')
                    favorite_param = shop_description_html_obj.xpath(u"//li[@id='J_SCollCount'][@data-info]/@data-info")
                    if favorite_param:
                        the_param = favorite_param[0].split('&')
                        favorite_url = "%s?t=%s&keys=%s&callback=setShopStat" % (
                            the_param[1].split('=')[1], str(int(time.time() * 1000)),
                            the_param[0].split('=')[1]
                        )
                        favorite_html = download_with_referer(favorite_url, shop_description_url)
                        if favorite_html:
                            the_shop.favorited_user_count = int(favorite_num_reg.search(favorite_html).group(1))
                            favorite_count_success = True



        if not favorite_count_success:
            logger.error("get shop favorite count failed. shop_id: %d." % the_shop.get_shop_id())
    except:
        logger.error("get shop favorite count failed. shop_id: %s. error info: %s" % (the_shop.get_shop_id(),  traceback.format_exc()))


def get_taobao_shop_extend_info(the_shop, shop_html_obj, item_html_obj, urls):
    """获取淘宝店铺扩展信息"""

    #当前主营 \u5f53\u524d\u4e3b\u8425
    the_shop.main_category = shop_html_obj.xpath("//li[contains(text(),'当前主营')]/a/text()".decode('utf-8'))[0]\
        .strip(u'\xa0').encode('utf-8').strip(" \t\n\r")

    #所在地区: \u6240\u5728\u5730\u533a\uff1a
    the_shop.location = format_location_reg.sub('', shop_html_obj.xpath("//li[contains(text(),'所在地区')]/text()".decode('utf-8'))[0]\
        .strip(u'\xa0').encode('utf-8')).strip(" \t\n\r")

    #创店时间 \u521b\u5e97\u65f6\u95f4 此处页面上使用js写入
    the_shop.open_at = datetime.datetime.strptime(
        shop_html_obj.xpath("//input[@id='J_showShopStartDate']/@value")[0].encode('utf-8'), "%Y-%m-%d")

    #好评率 \u597d\u8bc4\u7387
    the_shop.good_item_rate = float(\
        format_good_item_rate_reg.sub('', shop_html_obj.xpath(\
        "//div[@class='personal-rating col-main']/div[@class='main-wrap']//*[contains(text(),'好评率')]/text()".decode('utf-8'))[0]\
        .strip(u'\xa0').encode('utf-8')).strip(" \t\n\r%"))

    #宝贝与描述相符 \u5b9d\u8d1d\u4e0e\u63cf\u8ff0\u76f8\u7b26
    described_remark_result = get_compare_rate(shop_html_obj, "宝贝与描述相符".decode('utf-8'))
    the_shop.described_remark = described_remark_result[0]
    the_shop.described_remark_compare = described_remark_result[1]

    #卖家的服务态度 \u5356\u5bb6\u7684\u670d\u52a1\u6001\u5ea6
    service_remark_result = get_compare_rate(shop_html_obj, "卖家的服务态度".decode('utf-8'))
    the_shop.service_remark = service_remark_result[0]
    the_shop.service_remark_compare = service_remark_result[1]

    #卖家发货的速度 \u5356\u5bb6\u53d1\u8d27\u7684\u901f\u5ea6
    shipping_remark_result = get_compare_rate(shop_html_obj, "卖家发货的速度".decode('utf-8'))
    the_shop.shipping_remark = shipping_remark_result[0]
    the_shop.shipping_remark_compare = shipping_remark_result[1]

    if len(shop_html_obj.xpath(u"//div[@class='xiaobao-box']//span[@class='xiaofei']/@class")):
        the_shop.support_consumer_guarantees = 1
    else:
        the_shop.support_consumer_guarantees = 0

    if len(shop_html_obj.xpath(u"//div[@class='xiaobao-box']//span[@class='seven']/@class")):
        the_shop.support_returnin7day = 1
    else:
        the_shop.support_returnin7day = 0

    #信用卡 \u4fe1\u7528\u5361 信用卡付款 \u4fe1\u7528\u5361\u4ed8\u6b3e
    if len(item_html_obj.xpath("//div[@class='shop-details']//li[@class='honours']//img[contains(@title,'信用卡')]/@title".decode('utf-8')))\
        or len(shop_html_obj.xpath("//div[@class='qualification-service has-hover']//a[contains(text(),'信用卡付款')]/text()".decode('utf-8'))):
        the_shop.support_credit_card = 1
    else:
        the_shop.support_credit_card = 0

    #货到付款 \u8d27\u5230\u4ed8\u6b3e
    if len(item_html_obj.xpath("//div[@class='shop-details']//li[@class='honours']//img[contains(@title,'货到付款')]/@title".decode('utf-8')))\
        or len(shop_html_obj.xpath("//div[@class='qualification-service has-hover']//a[contains(text(),'货到付款')]/text()".decode('utf-8'))):
        the_shop.support_cash = 1
    else:
        the_shop.support_cash = 0

    get_taobao_shop_favorite_count(the_shop, shop_html_obj, urls)


def get_tmall_shop_collected_count(the_shop, shop_html_obj, item_html_obj,  urls):
    """获取天猫店被关注数目"""
    try:
        is_done = False

        if not is_done:
            collected_count = shop_html_obj.xpath(u"//em[@class='j_CollectBrandNum']/text()")
            if collected_count and collected_count[0].isdigit():
                the_shop.favorited_user_count = int(collected_count[0])
                is_done = True

        if not is_done:
            shop_home_html = download_with_referer(urls['shop_url'], None)
            shop_home_obj = parse_html(shop_home_html, 'gbk')
            collected_count = shop_home_obj.xpath(u"//em[@class='j_CollectBrandNum']/text()")
            if collected_count and collected_count[0].isdigit():
                the_shop.favorited_user_count = int(collected_count[0])
                is_done = True

        if not is_done:
            collected_count = item_html_obj.xpath(u"//em[@class='j_CollectBrandNum']/text()")
            if collected_count and collected_count[0].isdigit():
                the_shop.favorited_user_count = int(collected_count[0])
                is_done = True

        if not is_done:
            logger.error("get shop collected count failed. shop_id: %d." % the_shop.get_shop_id())
    except:
        logger.error("get shop favorite count failed. shop_id: %s. error info: %s" % (the_shop.get_shop_id(),  traceback.format_exc()))


def get_tmall_shop_extend_info(the_shop, shop_html_obj, item_html_obj, urls):
    """获取天猫店铺扩展信息"""
    #当前主营 \u5f53\u524d\u4e3b\u8425
    the_shop.main_category = shop_html_obj.xpath("//li[contains(text(),'当前主营')]/a/text()".decode('utf-8'))[0]\
        .strip(u'\xa0').encode('utf-8').strip(" \t\n\r")

    #所在地区: \u6240\u5728\u5730\u533a\uff1a
    the_shop.location = format_location_reg.sub('', shop_html_obj.xpath("//li[contains(text(),'所在地区')]/text()".decode('utf-8'))[0]\
        .strip(u'\xa0').encode('utf-8')).strip(" \t\n\r")

    #宝贝与描述相符 \u5b9d\u8d1d\u4e0e\u63cf\u8ff0\u76f8\u7b26
    described_remark_result = get_compare_rate(shop_html_obj, "宝贝与描述相符".decode('utf-8'))
    the_shop.described_remark = described_remark_result[0]
    the_shop.described_remark_compare = described_remark_result[1]

    #卖家的服务态度 \u5356\u5bb6\u7684\u670d\u52a1\u6001\u5ea6
    service_remark_result = get_compare_rate(shop_html_obj, "卖家的服务态度".decode('utf-8'))
    the_shop.service_remark = service_remark_result[0]
    the_shop.service_remark_compare = service_remark_result[1]

    #卖家发货的速度 \u5356\u5bb6\u53d1\u8d27\u7684\u901f\u5ea6
    shipping_remark_result = get_compare_rate(shop_html_obj, "卖家发货的速度".decode('utf-8'))
    the_shop.shipping_remark = shipping_remark_result[0]
    the_shop.shipping_remark_compare = shipping_remark_result[1]

    get_tmall_shop_collected_count(the_shop, shop_html_obj, item_html_obj, urls)


def crawl_one_shop(shop, db):

    shop_id = shop[0]
    shop_type = shop[1]
    shop_url = shop[2]
    first_item_id = shop[3]
    item_html_id = shop[4]
    item_html = shop[5]

    urls = {'shop_url': shop_url}

    try:
        the_shop = ShopExtendInfo(db, shop_id)
        result = False

        logger.info("begin get shop extend info. shop id: %d. shop type: %d." % (shop_id, shop_type))
        logger.debug("first item id: %d. item html id: %d. html length: %d" % (first_item_id, item_html_id, len(item_html)))

        url = url_reg.search(item_html).group(1).encode('utf-8')
        urls['shop_rate_url'] = url

        shop_html = download_with_referer(url, shop_url)

        if shop_html:
            logger.debug("download shop extend html. shop_id: %d, item_id: %d, url: %s. length: %d"
                % (shop_id, first_item_id, url, len(shop_html)))

            shop_html_obj = parse_html(shop_html, 'gbk')
            item_html_obj = parse_html(item_html, 'gbk')
            if shop_type == 1:
                get_taobao_shop_extend_info(the_shop, shop_html_obj, item_html_obj, urls)
            else:
                get_tmall_shop_extend_info(the_shop, shop_html_obj, item_html_obj, urls)

            the_shop.save()
            result = True
        else:
            logger.error("download shop extend html error. shop_id: %d, item_id: %d, url: %s." % (shop_id, first_item_id, url))

        if result:
            logger.info("success get shop extend info. shop_id: %d. type: %d." % (shop_id, shop_type))
        else:
            logger.error("fail get shop extend info. shop_id: %d.  type: %d." % (shop_id, shop_type))

    except:
        logger.error("update_shop_extend_info failed. shop_id: %s. type: %d, error info: %s" % (shop_id, shop_type, traceback.format_exc()))


def crawl_shops(sql_filter):

    sql_template = '''
select s.id as shop_id
, s.type as shop_type
, s.url as shop_url
, i.id as first_item_id
, h.id as item_html_id
, h.html as item_html
from
(
    select max(i.id) as item_id , i.shop_id FROM item i
    inner join crawl_html h on i.status = 1 and i.crawl_status = 2 and i.id = h.item_id
    group by i.shop_id
) sni
inner join item i on sni.item_id = i.id
inner join crawl_html h on h.item_id = i.id
inner join shop s on i.shop_id = s.id
where
'''
    sql = sql_template + sql_filter + ';'

    db_shop = get_db_engine()
    shops = db_shop.execute(sql)

    if not shops.returns_rows:
        logger.info("no shops to be crawled.")
        return

    # debug
    if FLAGS.debug_parser:
        import pdb
        pdb.set_trace()

    db = get_db_engine()
    last_time = 0
    for shop in shops:
        cur = time.time() * 1000
        if cur - last_time < FLAGS.interval:
            time.sleep((FLAGS.interval - (cur - last_time)) / 1000.0)
        last_time = time.time() * 1000
        crawl_one_shop(shop, db)

    #此处程序结束 所以没有考虑try finally显示释放数据库链接


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")

    if FLAGS.shop_ids:
        crawl_shops(" s.id in( %s ) " % FLAGS.shop_ids)
    else:
        if FLAGS.force:
            crawl_shops(" s.type <= 2 ")
        else:
            crawl_shops(" s.status = 1 and s.type <= 2 ")

