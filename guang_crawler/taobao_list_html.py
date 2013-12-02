#coding:utf8

import logging
import re
import gflags
import traceback
import urllib2
import time
from urlparse import urljoin

from pygaga.helpers.cachedns_urllib import custom_dns_opener
from pygaga.helpers.urlutils import download, parse_html, BannedException
from pygaga.helpers.statsd import statsd_timing

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_boolean('dumperrhtml', False, "dump err html")

DEFAULT_UA="Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
urllib2.install_opener(custom_dns_opener())

PRICE_RE = re.compile("([0-9\.]+)")
IID_RE = re.compile("id=(\d+)")
ITEM_NUMBER_RE = re.compile(u"宝贝数量：\s*<[^>]+>\s*(\d+)\s*<".encode('gbk'))

class ShopOfflineException(Exception):
    def __init__(self, value):
        self.value = value

class TaobaoListHtml:
    def __init__(self, shop_id, shop_url):
        self.id = shop_id
        self.url = shop_url
        self.list_url = urljoin(self.url, "search.htm?search=y&viewType=grid&orderType=_newOn&pageNum=1")
        if self.url.find('tmall.com') > 0:
            self.is_tmall = True
        else:
            self.is_tmall = False
        self.headers = {'Referer' : self.list_url,
                        'User-Agent' : DEFAULT_UA,
                        #'Accept-Encoding': 'gzip, deflate',
                        #'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        #'Accept-Language': 'en-us,en;q=0.5',
                        #'Connection' : 'keep-alive'
                        }
        self.total_items = []
        self.success = False
        self.err_reason = ""
        self.iids = set()
        self.count = 0
        self.level_img = []
        self.nick_url = ''

    def crawl(self, maxpage=0):
        result, items, pages, data = self.crawl_page(self.list_url)
        if not result or (not items and len(result) == 0):
            if FLAGS.dumperrhtml:
                dumpf = open("%s_%s" % (self.id, self.url.replace('/', '_').replace(':','_').replace('&','_').replace('?','_')), 'w')
                dumpf.write(data)
                dumpf.close()
            self.err_reason = "No total numbers found"
            raise Exception(self.err_reason)

        count = int(result[0].strip())
        self.count = count
        if not items or count == 0:
            return

        logger.info("crawling %s %s", self.id, count)

        item_objs = self.process_items(items)
        self.total_items.extend(item_objs)
        iids = [item['iid'] for item in item_objs]
        self.iids.update(iids)
        per_page_num = len(set(iids))
        logger.debug("crawling total %s perpage %s page 1 %s -> %s", count, per_page_num, len(item_objs), len(self.iids))

        if maxpage == 0:
            maxpage = (count+per_page_num-1)/per_page_num+1
        for pageno in range(2, maxpage):
            retry_count = 2
            while retry_count > 0:
                if FLAGS.interval > 100:
                    time.sleep(FLAGS.interval/1000.0)
                result2, items2, pages2, data = self.crawl_page(urljoin(self.url, "search.htm?search=y&viewType=grid&orderType=_newOn&pageNum=%s" % pageno))

                if not items2 and not result2 and not pages2:
                    self.err_reason = "download failed, parse none"
                    retry_count -= 1
                    if retry_count > 0:
                        continue
                    if FLAGS.dumperrhtml:
                        dumpf = open("%s_%s" % (self.id, self.url.replace('/', '_').replace(':','_').replace('&','_').replace('?','_')), 'w')
                        dumpf.write(data)
                        dumpf.close()
                    raise Exception(self.err_reason)

                item_objs = self.process_items(items2)
                iids = set([item['iid'] for item in item_objs])
                logger.debug("crawling page %s %s -> %s intersect %s", pageno, len(item_objs), len(iids), len(self.iids.intersection(iids)))

                # 返回一个新的set包含 iids 中有但是 self.iids 中没有的元素,这就是需要抓取的
                # 这样的话，重复的商品就不抓了，至少能保证能抓取到部分新商品
                iids = iids - self.iids     # or s.difference(t)

                """
                # 这里原来是判断2个numiids相交 >5 时放弃抓取该店铺
                if len(self.iids.intersection(iids)) > 5 and not (pageno>=100):
                    self.err_reason = "Duplicated items %s - %s" % (pageno, len(self.iids.intersection(iids)))
                    raise Exception(self.err_reason)
                """

                self.total_items.extend(item_objs)
                self.iids.update(iids)
                break

        self.success = True
        logger.info("crawled %s results %s %s", self.id, count, len(self.total_items))

    def get_level(self):
        if(self.is_tmall):
            return "tmall.png"

        level_img = ""
        if(self.level_img and len(self.level_img) > 0):
            level_img = self.level_img[0]
            level_img = level_img.split('/')[-1]
        return level_img

    def process_items(self, items):
        results = []
        iid_set = set()
        for item in items:
            data = {}
            try:
                data['desc'] = item.xpath("div[@class='desc']/a/text()")[0].strip()
                data['href'] = item.xpath("div[@class='desc']/a/@href")[0]
                data['iid'] = IID_RE.findall(data['href'])[0]
                data['price'] = float(PRICE_RE.findall(item.xpath("div[@class='price']/strong/text()")[0])[0])
                try:
                    data['sales_amount'] = int(item.xpath("div[@class='sales-amount']/em/text()")[0].strip())
                except:
                    data['sales_amount'] = -1
                if data['iid'] in iid_set: # remove duplicate items
                    continue
                if data['desc'].find(u'邮费链接') >= 0 or data['desc'].find(u'差价链接') >= 0:
                    continue
                if data['price'] < 1.01:
                    logger.info("Price maybe error %s %s", self.id, data)
            except:
                try:
                    data['desc'] = item.xpath("dt[@class='photo']/a/img/@alt")[0].strip()
                    data['href'] = item.xpath("dd[@class='detail']/a/@href")[0]
                    data['iid'] = IID_RE.findall(data['href'])[0]
                    data['price'] = float(item.xpath("dd[@class='detail']//span[@class='c-price']/text()")[0].strip())
                    try:
                        data['sales_amount'] = int(item.xpath("dd[@class='detail']//span[@class='sale-num']/text()")[0].strip())
                    except:
                        data['sales_amount'] = -1
                except:
                    logger.error("crawling list %s unknown exception %s", self.id, traceback.format_exc(), extra={'tags':['crawlParseListItemException',]})
            if data.has_key('iid'):
                iid_set.add(data['iid'])
                results.append(data)
                #logger.debug("Got %s %s %s", data['iid'], data['desc'], data['price'])
        return results

    def crawl_page(self, url):
        retry_count = 1
        while retry_count >= 0:
            try:
                data = self.download_page(url)
                if not data:
                    logger.error("crawl %s %s failed", self.id, url)
                    return None, None, None, None
                if FLAGS.dump:
                    dumpf = open("%s_%s" % (self.id, url.replace('/', '_').replace(':','_').replace('&','_').replace('?','_')), 'w')
                    dumpf.write(data)
                    dumpf.close()
                if FLAGS.debug_parser:
                    import pdb; pdb.set_trace()
                if data.find(u"没有找到相应的店铺信息".encode('gbk')) > 0:
                    logger.warn("Shop %s is offline %s", self.id, self.url)
                    raise ShopOfflineException(data)

                html_obj = parse_html(data, encoding="gb18030")

                self.level_img = html_obj.xpath("//img[@class='rank']/@src")
                self.nick_url = html_obj.xpath("//a[@class='shop-name']/@href")
                if not self.nick_url:
                    self.nick_url = html_obj.xpath("//div[@id='shop-info']//a/@href")

                result = html_obj.xpath("//div[@id='anchor']//div[@class='search-result']//span/text()")
                items = html_obj.xpath("//div[@id='anchor']//div[@class='item']")
                pages = html_obj.xpath("//div[@id='anchor']//div[@class='pagination']/a[@class='J_SearchAsync']/@href")
                if not result:
                    result = ITEM_NUMBER_RE.findall(data)
                    if result and not items:
                        items = html_obj.xpath("//ul[@class='shop-list']//div[@class='item']")
                if not result:
                    result = html_obj.xpath("//div[@id='J_ShopSearchResult']//div[@class='search-result']//span/text()")
                    items = html_obj.xpath("//div[@id='J_ShopSearchResult']//dl[contains(@class, 'item')]")
                    pages = html_obj.xpath("//div[@id='J_ShopSearchResult']//div[@class='pagination']/a[@class='J_SearchAsync']/@href")
                if not result:
                    result = html_obj.xpath("//form[@id='shop-search-list']//div[@class='search-result']//span/text()")
                    if result and not items:
                        items = html_obj.xpath("//div[@class='shop-hesper-bd grid']//dl[@class='item']")
                        pages = html_obj.xpath("//div[@class='pagination']/a[@class='J_SearchAsync']/@href")

                if not result:
                    # pageLen = ['1/107']
                    pageLen = html_obj.xpath("//p[@class='ui-page-s']//b[@class='ui-page-s-len']/text()")
                    items = html_obj.xpath("//div[@class='J_TItems']//dl[contains(@class, 'item')]")
                    c = 0
                    if "/" in pageLen[0]:
                        c = int(pageLen[0].split("/")[1].strip()) * len(items)
                    else:
                        c = int(pageLen[0].strip()) * len(items)
                    result.append(str(c))
                    pages = html_obj.xpath("//div[@class='J_TItems']//div[@class='pagination']/a[@class='J_SearchAsync']/@href")

                if not result and not items and not pages:
                    logger.error("crawl %s %s -- 0 items found, page len %s", self.id, url, len(data))
                    if retry_count > 0 and len(data) < 1024:
                        retry_count -= 1
                        time.sleep(1.0)
                        continue
                return result, items, pages, data
            except ShopOfflineException, e:
                raise e
            except BannedException, e:
                raise e
            except:
                logger.error("crawling list %s unknown exception %s", self.id, traceback.format_exc(), extra={'tags':['crawlListException',]})
                raise
        return None, None, None, None

    @statsd_timing('guang.crawl.listpage')
    def download_page(self, url, max_retry_count=5):
        result = download(url, max_retry=max_retry_count,
                          fn_is_banned=lambda data:data.find(u"您的访问受到限制".encode('gbk')) > 0,
                          throw_on_banned=True)
        return result

if __name__ == "__main__":
    from pygaga.helpers.logger import log_init

    gflags.DEFINE_boolean('debug_parser', False, "debug html parser?")
    gflags.DEFINE_interval('interval', 100, "crawl page interval in ms")

    FLAGS.stderr = True
    FLAGS.verbose = "debug"
    FLAGS.color = True
    log_init("CrawlLogger", "sqlalchemy.*")

    tb2 = TaobaoListHtml(1, "http://juzigongfang.taobao.com/")
    tb2.crawl()

    tb = TaobaoListHtml(1, "http://o2shop.tmall.com/")
    tb.crawl()
