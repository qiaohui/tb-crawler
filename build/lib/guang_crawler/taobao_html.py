#coding:utf8

import logging
import re
import gflags
import traceback
import urllib2
import socket
import time
import datetime

from decimal import Decimal
from pygaga.helpers.cachedns_urllib import custom_dns_opener
from pygaga.helpers.urlutils import download, parse_html
from pygaga.helpers.statsd import statsd_timing
from pygaga.simplejson import loads
from pygaga.helpers.utils import get_val, get_num_val

from guang_crawler import comments_pb2

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

DEFAULT_UA="Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
urllib2.install_opener(custom_dns_opener())

CURPAGE_RE = re.compile("^(.*currentPage=)([^&]*?)(&.*|)$")

JSON_RE = re.compile("^\s*jsonp_reviews_list\((.+)\)\s*$", re.M|re.S)
TM_JSON_RE=re.compile("^\s*TB.detailRate\s*=\s*(.+)$", re.M|re.S)
RATECOUNT_RE=re.compile("^.*<em>([0-9]+)</em>.*$", re.M|re.S)
DESCURL_RE = re.compile("http://dsc.taobaocdn.com/i\d[^\"']+\.desc[^\"']*", re.M|re.S)
IMAGESTYLE_RE = re.compile("^.*url\(([^\)]+)\)$", re.M|re.S)

class TaobaoHtml:
    def __init__(self, item_id, num_id, is_tmall=False, max_comments=0):
        self.item_id = item_id
        self.num_id = num_id
        self.is_tmall = is_tmall
        if self.is_tmall:
            self.url = "http://detail.tmall.com/item.htm?id=%s" % num_id
        else:
            self.url = "http://item.taobao.com/item.htm?id=%s" % num_id
        self.headers = {'Referer': self.url, 'User-Agent': DEFAULT_UA}
        self.data = ""
        self.html_obj = None
        self.detailDiv = self.buyButton = self.originPrice = []
        self.thumbImages = []
        self.comments = []
        self.descUrl = self.descContent = ""
        self.promoteUrl = self.promoteContent = self.promoteUrl2 = ""
        self.price = 0.0
        self.is_offline = False
        self.tmallInitApijson = None
        self.volume = -1
        self.max_comments = max_comments

    @statsd_timing('guang.crawl.details.%s' % socket.gethostname())
    def crawl_page(self, url):
        result = download(url, self.headers)
        return result

    def check_offline(self):
        return self.data.find(u"此商品已下架".encode('gbk')) > 0 or self.data.find(u"我的闲置".encode('gbk')) > 0 or self.data.find(u"您查看的宝贝不存在".encode('gbk')) > 0 or self.data.find("tbid-container") > 0 or self.data.find(u"此宝贝已下架".encode("gbk")) > 0

    def crawl(self):
        try:
            self.data = self.crawl_page(self.url)
            if FLAGS.debug_parser:
                import pdb; pdb.set_trace()

            # check tmall
            if not self.is_tmall and len(self.data) < 256 and self.url.find('item.taobao.com') > 0 and self.data.find("window.location.href='http://detail.tmall.com/item.htm'+window.location.search") > 0:
                self.data = self.crawl_page(self.url.replace('item.taobao.com', 'detail.tmall.com'))

            if self.check_offline():
                self.is_offline = True

            self.html_obj = parse_html(self.data, encoding="gb18030")

            title = self.html_obj.xpath("//html/head/title/text()")
            if title and title[0].find(u"转卖") > 0:
                self.is_offline = True

            self.detailDiv = self.html_obj.xpath("//div[@id='detail']")
            self.buyButton = self.html_obj.xpath("//a[@id='J_LinkBuy']")
            self.originPrice = self.html_obj.xpath("//strong[@id='J_StrPrice']/em[@class='tb-rmb-num']/text()")
            if not self.originPrice:
                self.originPrice = self.html_obj.xpath("//strong[@class='J_originalPrice']/text()")
            #self.bidPrice = self.html_obj.xpath("//li[contains(concat(' ',normalize-space(@class),' '),' detail-price ')]/strong/text()")
            self.bidPrice = self.html_obj.xpath("//input[@name='current_price']/@value")
            self.thumbImages = self.html_obj.xpath("//ul[@id='J_UlThumb']//img/@src")
            if not len(self.thumbImages):
                try:
                    # try load thumb images for tmall page
                    self.thumbImages = [IMAGESTYLE_RE.subn(r'\g<1>', x)[0] for x in self.html_obj.xpath("//ul[@id='J_UlThumb']//li/@style")]

                    # taobao @src to @data-src
                    if not len(self.thumbImages):
                        self.thumbImages = self.html_obj.xpath("//ul[@id='J_UlThumb']//img/@data-src")
                except:
                    logger.warn("No thumbs found %s", self.item_id)

            tblogo = self.html_obj.xpath("//*[@id='shop-logo']")
            tmalllogo = self.html_obj.xpath("//*[@id='mallLogo']")
            if not self.is_tmall and tmalllogo:
                self.is_tmall = True

            if self.is_tmall:
                self.cid = get_val(self.data, "categoryId").split('&')[0]

                apiItemInfoUrl = get_val(self.data, "initApi").replace(r'''\/''', "/")
                self.tmallInitApi = self.crawl_page(apiItemInfoUrl)
                try:
                    self.tmallInitApijson = loads(self.tmallInitApi.decode('gb18030').encode('utf8'))
                except:
                    logger.info("parse tmall api json failed %s : %s", self.item_id, traceback.format_exc())
                if self.tmallInitApijson:
                    try:
                        self.volume = self.tmallInitApijson['defaultModel']['sellCountDO']['sellCount']
                    except:
                        logger.warn("try to get volume from api failed %s", self.item_id)
                if self.volume < 0:
                    try:
                        self.volume = int(get_val(self.tmallInitApi, "sellCount"))
                    except:
                        logger.warn("Can not parse item volume %s", self.item_id)

                # 库存 ：icTotalQuantity
                """"
                reviewInfoUrl = get_val(self.data, "apiMallReviews").replace(r'''\/''', "/")
                reviewInfoData = self.crawl_page(reviewInfoUrl)
                m = RATECOUNT_RE.match(reviewInfoData)
                if m:
                    self.reviewCount = m.group(1)
                else:
                    self.reviewCount = None
                """
            else:
                self.cid = get_val(self.data, "cid")

                apiItemInfoVal = get_val(self.data, "apiItemInfo")
                if apiItemInfoVal:
                    apiItemInfoUrl = get_val(self.data, "apiItemInfo").replace(r'''\/''', "/")
                    itemInfoData = self.crawl_page(apiItemInfoUrl)
                    try:
                        self.volume = int(get_num_val(itemInfoData, 'quanity'))
                    except:
                        self.volume = -1
                else:
                    self.volume = -1

                #interval = get_val(data2, 'interval')
                # 库存 skudata = get_val(self.data, 'valItemInfo').replace(r'''\/''', "/")
                """
                reviewInfoUrl = get_val(self.data, "data-commonApi").replace(r'''\/''', "/")
                reviewInfoData = self.crawl_page(reviewInfoUrl)
                self.reviewCount = get_val(reviewInfoData, 'total')
                """
        except:
            logger.error("crawling %s unknown exception %s", self.item_id, traceback.format_exc(), extra={'tags':['crawlItemException',]})
            raise

    def crawl_desc(self):
        try:
            self.descUrl = get_val(self.data, "apiItemDesc").replace(r'''\/''', "/")
        except:
            try:
                self.descUrl = get_val(self.data, "ItemDesc").replace(r'''\/''', "/")
            except:
                if not self.data.find(u"暂无描述".encode('gb18030')) > 0:
                    self.descUrl = DESCURL_RE.search(self.data).group(0)
        # find http://dsc.taobaocdn.com/[^.]+\.desc.* as descUrl
        if self.descUrl:
            self.descContent = self.crawl_page(self.descUrl)
            logger.debug("Got %s desc details %s", self.item_id, len(self.descContent))
        else:
            logger.warn("%s desc url not found", self.item_id)

    def crawl_price(self):
        self.promoteUrl2 = get_val(self.data, "apiPromoData")
        if self.promoteUrl2:
            self.promoteUrl2 = self.promoteUrl2.replace(r'''\/''', "/")

        price = ""
        if self.is_tmall and self.tmallInitApi and self.tmallInitApijson:
            try:
                priceInfo = self.tmallInitApijson['defaultModel']['itemPriceResultDO']['priceInfo']
                if priceInfo:
                    if priceInfo.has_key('def'):
                        defaultPriceInfo = priceInfo['def']
                    else:
                        defaultPriceInfo = priceInfo[priceInfo.keys()[0]]

                    if defaultPriceInfo.has_key('promPrice'):
                        price = defaultPriceInfo['promPrice']['price']
                    elif defaultPriceInfo.has_key('promotionList') and defaultPriceInfo['promotionList']:
                        price = str(min([float(x.get('price','100000000.0')) for x in defaultPriceInfo['promotionList']]))
                    else:
                        price = defaultPriceInfo['price']
            except:
                logger.warn("Parse tmall json price failed, %s", self.item_id)

        if not price:
            if self.promoteUrl2:
                self.promoteContent = self.crawl_page(self.promoteUrl2).replace('&quot;', '"')
                tag = "low:"
                if self.promoteContent.find(tag) > 0:
                    pos = self.promoteContent.find(tag) + len(tag)
                    pos2 = self.promoteContent.find(',', pos)
                    price = self.promoteContent[pos:pos2]
                if not price:
                    price = get_num_val(self.promoteContent, 'price')
            else:
                self.promoteUrl = "http://marketing.taobao.com/home/promotion/item_promotion_list.do?itemId=%s" % self.num_id
                self.promoteContent = self.crawl_page(self.promoteUrl).replace('"', '&quot;')
                tag = "promPrice&quot;:&quot;"
                if self.promoteContent.find(tag) > 0:
                    pos = self.promoteContent.find(tag) + len(tag)
                    pos2 = self.promoteContent.find('&quot;', pos)
                    price = self.promoteContent[pos:pos2]

        if not price:
            tbPrice = self.html_obj.xpath("//strong[@class='tb-price']/text()")
            tbPrice1 = self.html_obj.xpath("//span[@class='tb-price']/text()")
            if tbPrice and not tbPrice[0].strip():
                price = tbPrice[0].strip()
            elif tbPrice1 and not tbPrice1[0].strip():
                price = tbPrice1[0].strip()

        if price.find("-") > 0:
            price = price.split('-')[0].strip()

        # 2013-09-03  get price url
        if not price:
            #这里稍微有点麻烦,主要针对string进行处理
            pirce_url = "http://ajax.tbcdn.cn/json/umpStock.htm?itemId=%s&p=1" % self.num_id
            response = download(pirce_url, self.headers)
            rg = re.compile('price:\"[0-9]+[.][0-9]+\"', re.IGNORECASE|re.DOTALL)
            m = rg.search(response.decode('gb18030').encode('utf8'))
            if m:
                price_str = m.group(0).split(":")[1].replace("\"", "")
                price = Decimal(price_str)

        # not chuxiao price, set origin price
        if not price:
            if self.originPrice:
                price = self.originPrice[0].strip()
            elif self.bidPrice:
                price = self.bidPrice[0].strip()
            if price.find("-") > 0:
                price = price.split('-')[0].strip()

        self.price = float(price)
        logger.debug("%s price is %s", self.item_id, self.price)

    def cut_comments(self, from_rate_id):
        last_id = self.comments[-1].rateid
        if last_id <= from_rate_id:
            self.comments = [c for c in self.comments if c.rateid > from_rate_id]
            return True
        else:
            return False

    def crawl_rate(self, from_rate_id=0):
        if self.is_tmall:
            self.crawl_tmall_rate(from_rate_id)
        else:
            self.crawl_taobao_rate(from_rate_id)

    def crawl_taobao_rate(self, from_rate_id):
        rateListUrlBase = get_val(self.data, "data-listApi")
        if rateListUrlBase:
            rateListUrlBase = rateListUrlBase.replace(r'''\/''', "/")

        maxPage = 1
        page = 1
        total = 0
        while page <= maxPage:
            if page >= FLAGS.mostPage:
                break
            page1Result = self.crawl_taobao_rate_page(rateListUrlBase, page)
            if not page1Result:
                return

            results = page1Result['comments']
            maxPage = page1Result['maxPage']
            if not results:
                break
            self.taobao_comments_to_pb(results)
            page += 1
            total += len(results)
            if from_rate_id:
                if self.cut_comments(from_rate_id):
                    break
            if self.max_comments > 0 and self.max_comments < total:
                break
        logger.debug("Got %s %s comments", self.item_id, len(self.comments))

    def crawl_taobao_rate_page(self, rateListUrlBase, page):
        if rateListUrlBase:
            rateListUrl = rateListUrlBase + '&currentPageNum=%s&rateType=&orderType=feedbackdate&showContent=1&attribute=&callback=jsonp_reviews_list' % page
            rate1 = self.crawl_page(rateListUrl)
            m = JSON_RE.match(rate1)
            if m:
                jsonobj = loads(m.group(1).decode('gb18030').encode('utf8'))
                return jsonobj
        return None

    def taobao_comments_to_pb(self, items):
        for s in items:
            c = comments_pb2.comments()
            c.user = s['user']['nick']
            c.content = s['content']
            try:
                c.time = int(time.mktime(datetime.datetime.strptime(s['date'],'%Y.%m.%d').timetuple()))
            except:
                c.time = int(time.mktime(datetime.datetime.strptime(s['date'].encode('utf8'),u'%Y\u5e74%m\u6708%d\u65e5 %H:%M'.encode('utf8')).timetuple()))
            if s['rate'] == "1":
                c.result = 1
            elif s['rate'] == "0":
                c.result = 2
            elif s['rate'] == "-1":
                c.result = 3
            c.rateid = s['rateId']
            try:
                c.userid = int(s['user']['userId'])
            except:
                pass
            c.userrank = s['user']['rank']
            if s['user']['vipLevel']:
                c.userviplevel = s['user']['vipLevel']
            self.comments.append(c)

    def crawl_tmall_rate(self, from_rate_id):
        rateListUrlBase = "http://rate.tmall.com/list_detail_rate.htm?itemId=%s&order=1&append=0&content=1&tagId=&posi=&currentPage=1" % self.num_id

        page = 1
        maxPage = 1
        total = 0
        while page <= maxPage:
            if page >= FLAGS.mostPage:
                break

            page1Result = self.crawl_tmall_rate_page(rateListUrlBase, page)
            if not page1Result:
                break
            results = page1Result['rateDetail']['rateList']
            if not results:
                break
            if maxPage == 1:
                maxPage = page1Result['rateDetail']['paginator']['lastPage']
            page += 1
            total += len(results)
            self.tmall_comments_to_pb(results)
            if from_rate_id:
                if self.cut_comments(from_rate_id):
                    break
            if self.max_comments > 0 and self.max_comments < total:
                break
        logger.debug("Got %s %s comments", self.item_id, len(self.comments))

    def crawl_tmall_rate_page(self, url, page):
        if url:
            url = CURPAGE_RE.subn(r"\g<1>%s\g<3>" % page, url)[0]
            rate1 = self.crawl_page(url)
            rate1 = "{" + rate1 + "}"
            if rate1:
                jsonobj = loads(rate1.decode('gb18030').encode('utf8'))
                return jsonobj
        return None

    def tmall_comments_to_pb(self, items):
        for s in items:
            c = comments_pb2.comments()
            c.user = s['displayUserNick']
            c.content = s['rateContent']
            c.userviplevel = s['userVipLevel']
            try:
                c.userid = s['displayUserNumId']
            except:
                pass
            c.time = int(time.mktime(datetime.datetime.strptime(s['rateDate'],'%Y-%m-%d %H:%M:%S').timetuple()))
            c.rateid = s['id']
            c.result = 1    #天猫无好中差评价，都给好评
            self.comments.append(c)

if __name__ == "__main__":
    from pygaga.helpers.logger import log_init

    gflags.DEFINE_boolean('debug_parser', False, "debug html parser?")

    FLAGS.stderr = True
    FLAGS.verbose = "info"
    FLAGS.color = True
    log_init("CrawlLogger", "sqlalchemy.*")

    tm = TaobaoHtml(2, 15211603329) #tmall
    tm.crawl()
    tm.crawl_price()
    tm.crawl_desc()
    #tm.crawl_rate()
    tm.crawl_rate(18403379387)
    for c in tm.comments:
        logger.debug("  %s %s %s %s %s %s", c.rateid, c.userid, c.result, c.time, c.user, c.content)

    tb = TaobaoHtml(1, 16096984759) #taobao
    tb.crawl()
    tb.crawl_price()
    tb.crawl_desc()
    #tb.crawl_rate()
    tb.crawl_rate(18204645634)
    for c in tb.comments:
        logger.debug("  %s %s %s %s %s %s", c.rateid, c.userid, c.result, c.time, c.user, c.content)
