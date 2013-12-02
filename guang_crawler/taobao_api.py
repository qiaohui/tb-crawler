#coding=utf8
import logging
import hmac
import random
import re
import time
import traceback
from hashlib import md5
import urllib
import gflags

import requests

from pyTOP import api
from pyTOP.api import TOPRequest
from pyTOP.errors import TOPException

from pygaga.helpers.ratelimit import chunks, waitlimit, batch
from pygaga.helpers.urlutils import  get_cookie_opener, DEFAULT_UA, download
from pygaga.helpers.statsd import Statsd
from pygaga.helpers.logger import log_init

gflags.DEFINE_boolean('debug_topapi', False, "Debug taoabo api")

FLAGS = gflags.FLAGS

logger = logging.getLogger('TaobaoLogger')

#TAOBAO_SECRET = "a2398f38dc1447b18707674c67bfa784"
#TAOBAO_KEY = "12289114" #siqi, not used

TAOBAO_SECRET2 = "648d23023d9a6bc41da6326a8ed7920e"
TAOBAO_KEY2 = "12525923" #简单网tb 风格女装精选 -- fenfen, 1000/min

TAOBAOKE_APPKEY = "12669715" # www.j.cn -- 淘宝客转换
TAOBAOKE_APPSECRET = "7062800942c7b6f18f6a393a364d812f"

TAOBAO_KEYSECRETS = {
            "12525923" : "648d23023d9a6bc41da6326a8ed7920e", # 简单网tb 风格女装精选 --> fenfen
            "12643639" : "7c244aed17eb69ecd1fb3178e124dd74", # 简单网tb 女装团购
            "12675810" : "760f45a8447cbc8acb3a1d73c63e570f"  # fakechris 女装推荐 --> i2fang
}

R_ERROR_MSG = re.compile(".*?(\d+).*seconds.*")

def get_top(key=TAOBAO_KEY2, secret=TAOBAO_SECRET2, env="product"):
   return api.TOP(key, secret, env)

def get_taobaoke_top(key=TAOBAOKE_APPKEY, secret=TAOBAOKE_APPSECRET, env="product"):
   return api.TOP(key, secret, env)

def get_rand_top(keysesc=TAOBAO_KEYSECRETS, env="product"):
    key, secret = random.choice(keysesc.items())
    return get_top(key, secret, env)

def try_execute(top, request, expire=600.0):
    current = time.time()
    interval = 60.0 # wait for 1 min
    ban_retry_count = 0
    http_retry_count = 0
    http_max_retry = 3
    while True:
        try:
            if FLAGS.debug_topapi:
                import pdb; pdb.set_trace()
            result = top.execute(request)
            Statsd.increment('guang.taobaoapi.%s.succ' % request.method_name.replace('.', '_'))
            logger.debug("calling %s(%s) --> %s", request.method_name, request.api_params, result)
            return result
        except requests.exceptions.ConnectionError, e1:
            logger.warn("Call api http failed %s", traceback.format_exc())
            Statsd.increment('guang.taobaoapi.%s.conn_err' % request.method_name.replace('.', '_'))
            http_retry_count += 1
            if http_retry_count > http_max_retry:
                return None
            else:
                time.sleep(interval)
        except TOPException, e:
            logger.warn("Call api top failed %s", traceback.format_exc())
            Statsd.increment('guang.taobaoapi.%s.api_err' % request.method_name.replace('.', '_'))
            if e.code in [4, 5, 6, 7, 8]: #  This ban will last for 71 more seconds
                m = R_ERROR_MSG.match(e.message) # e.args[0]
                if m:
                    try:
                        interval = int(m.group(1)) + 10.0
                    except:
                        interval = 60.0
                if ban_retry_count > 0:
                    interval += 60.0*ban_retry_count
                ban_retry_count += 1
                logger.info("Waiting and try after %s", interval)
                time.sleep(interval)
                if time.time() - current > expire:
                    logger.error("call %s timeout %s" % (request.method_name, time.time()-current))
                    return None
            elif e.code == 560: # 查询不到对应的用户信息 (code=560)
                return {'error':560}
            else:
                return None

class TopRequests:
    def __init__(self, req, resp):
        self.items = {}
        for r in req:
            self.items[int(r[1])] = {'req' : r, 'resp' : None}
        if not resp or not resp.has_key('tbk_items'):
            return
        for r in resp['tbk_items']['tbk_item']:
            if self.items.has_key(r['num_iid']):
                self.items[r['num_iid']]['resp'] = r

def get_report(top, date, session, pageno=1, pagesize=40, expire=600):
    # TODO: session support
    request = TOPRequest('taobao.taobaoke.rebate.report.get')
    request['session'] = session
    request['fields'] = 'trade_parent_id,trade_id,real_pay_fee,commission_rate,commission,' \
                        'app_key,outer_code,create_time,pay_time,pay_price,num_iid,item_title,' \
                        'item_num,category_id,category_name,shop_title,seller_nick'
    request['start_time'] = date
    request['span'] = 300
    request['page_no'] = pageno
    request['page_size'] = pagesize
    return try_execute(top, request, expire)

def get_taobao_shops(top, nicks, expire=600):
    request = TOPRequest('taobao.tbk.shops.detail.get')
    request['seller_nicks'] = nicks     # array
    request['fields'] = 'user_id,seller_nick,shop_title,pic_url,shop_url'
    results = try_execute(top, request, expire)
    return results

#这里有个坑，items中的每一个对象都必须有2个以上的参数，且下标1必须为num_iid
def get_taobao_items(top, items, fn_join_iids=lambda x:','.join(x), batch_size=40, calllimit=10, expire=600):
    """

        2013-11-15 停止使用
        request = TOPRequest('taobao.items.list.get')
        request['fields'] = "detail_url,cid,num_iid,title,nick,pic_url,num,price,has_showcase,approve_status,list_time,delist_time,modified,stuff_status,is_timing,post_fee,express_fee,ems_fee, has_discount,freight_payer"

    """
    request = TOPRequest('taobao.tbk.items.detail.get')
    for chunk in waitlimit(calllimit, 50.0, batch(items, batch_size)): # calllimit for minutes
        chunk = list(chunk)
        request['fields'] = "num_iid,seller_id,nick,title,price,volume,pic_url,item_url,shop_url,click_url"
        request['num_iids'] = fn_join_iids(chunk)
        results = try_execute(top, request, expire)
        logger.debug('Calling %s(%s) -> %s', request.method_name, request.api_params, results)
        yield TopRequests(chunk, results)

def get_deleted_items(top, items, fn_join_iids=lambda x:','.join(map(str, x)), batch_size=20, calllimit=60, expire=600):
    request = TOPRequest('taobao.items.list.get')
    for chunk in waitlimit(calllimit, 60.0, batch(items, batch_size)):
        chunk = list(chunk)
        request['fields'] = "num_iid,approve_status"
        request['num_iids'] = fn_join_iids(chunk)
        results = try_execute(top, request, expire)
        logger.debug('Calling %s(%s) -> %s', request.method_name, request.api_params, results)
        try:
            # TODO: request may not show in result
            for item in results['items']['item']:
                if item['approve_status'] != 'onsale':
                    yield item
        except:
            logger.warn("get_deleted_items %s", traceback.format_exc())

def get_promotion_info(top, itemid, expire=600.0):
    request = TOPRequest('taobao.ump.promotion.get')
    request['item_id'] = itemid
    return try_execute(top, request, expire) # ump_promotion_get_response:promotions:promotion_in_item/shop --> list[desc/name]

def convert_taobaoke_widget(items, fn_join_iids=lambda x:','.join(x), batch_size=40, calllimit=60, expire=600, outer_code='jcn', appkey=TAOBAOKE_APPKEY, appsec=TAOBAOKE_APPSECRET):
    ts = int(time.time()*1000)
    msg = appsec + 'app_key' + str(appkey) + "timestamp" + str(ts) + appsec
    sign = hmac.HMAC(appsec, msg).hexdigest().upper()
    headers = {'User-Agent' : DEFAULT_UA, 'Referer' : "http://www.j.cn/"}
    for chunk in waitlimit(calllimit, 60.0, chunks(items, batch_size)): # calllimit for minutes
        params = {'app_key' : appkey,
                  '_t_sys' : 'args=4',
                  'method' : 'taobao.taobaoke.widget.items.convert',
                  'sign' : sign,
                  'timestamp' : ts,
                  'fields' : "num_iid,nick,price,click_url,commission,commission_rate,commission_num,commission_volume,shop_click_url,seller_credit_score",
                  'callback' : 'TOP.io.jsonpCbs.t%s' % md5( str(random.random()) ).hexdigest()[:13],
                  'partner_id' : 'top-sdk-js-20120801',
        }
        params['num_iids'] = fn_join_iids(chunk)
        if outer_code:
            params['outer_code'] = outer_code
        url = "http://gw.api.taobao.com/widget/rest?%s" % urllib.urlencode(params)
        results = download(url, headers=headers)
        if results:
            Statsd.increment('guang.taobaoapi.widget_succ')
        else:
            Statsd.increment('guang.taobaoapi.widget_err')
        #logger.debug('Calling %s(%s) -> %s', request.method_name, request.api_params, results)
        yield (chunk, results)

def get_taobao_itemcats(top, cid, expire=600.0):
    request = TOPRequest('taobao.itemcats.get')
    request['cids'] = cid
    request['fields'] = 'cid,parent_cid,name,is_parent'
    return try_execute(top, request, expire)


def get_taobao_trade(top, trade_id, expire=600.0):
    request = TOPRequest('taobao.trade.get')
    request['fields'] = 'buyer_rate'
    taobao_result = try_execute(top, request, expire)
    print taobao_result

# http://api.taobao.com/apidoc/api.htm?spm=0.0.0.0.MdKfUr&path=scopeId:394-apiId:11132
def get_spmeffect_trade(top, datestr, page_detail=False, module_detail=False, expire=600.0):
    request = TOPRequest('taobao.spmeffect.get')
    request['date'] = datestr
    request['page_detail'] = page_detail
    request['module_detail'] = module_detail
    taobao_result = try_execute(top, request, expire)
    return taobao_result

""""
# deprecated api
def get_counts(top, nicks, expire=600.0):
    request = TOPRequest('taobao.items.get')
    request['fields'] = ['nicks']
    request['nicks'] = nicks
    results = try_execute(top, request, expire)
    logger.debug("taobao.items.get nicks %s %s", nicks, results)
    return results['total_results']

def get_items(top, nicks, pageNo=1, pageSize=200, expire=600.0):
    request = TOPRequest('taobao.items.get')
    request['fields'] = "detail_url,cid,num_iid,title,nick,pic_url,num,price,has_showcase,volume"
    request['page_no'] = pageNo
    request['page_size'] = pageSize
    request['nicks'] = nicks
    results = try_execute(top, request, expire)
    logger.debug("taobao.items.get nicks %s page %s,%s %s", nicks, pageNo, pageSize, results)
    return results #['items']['item']

def get_rate(top, nicks, iid, pageNo=1, pageSize=40, expire=600.0):
    request = TOPRequest('taobao.traderates.search')
    request['num_iid'] = iid
    request['seller_nick'] = nicks
    request['page_no'] = pageNo
    request['page_size'] = pageSize
    results = try_execute(top, request, expire)
    logger.debug("taobao.traderates.search %s %s page %s,%s %s", nicks, iid, pageNo, pageSize, results)
    return results #['trade_rates']['trade_rate']

def convert_taobaoke(top, items, pid='30146700', fn_join_iids=lambda x:','.join(x), batch_size=40, calllimit=60, expire=600, outer_code='jcn'):
    request = TOPRequest('taobao.taobaoke.items.convert')
    for chunk in waitlimit(calllimit, 60.0, chunks(items, batch_size)): # calllimit for minutes
        request['fields'] = "num_iid,nick,price,click_url,commission,commission_rate,commission_num,commission_volume,shop_click_url,seller_credit_score"
        request['num_iids'] = fn_join_iids(chunk)
        request['pid'] = pid
        if outer_code:
            request['outer_code'] = outer_code
        results = try_execute(top, request, expire)
        #logger.debug('Calling %s(%s) -> %s', request.method_name, request.api_params, results)
        yield (chunk, results)

def crawl_shop(top, nicks, page_size=200, calllimit=60, expire=600.0):
    count = get_counts(top, nick, expire)
    page_count = (count + page_size - 1) / page_size
    for page in waitlimit(calllimit, 60.0, xrange(1, page_count)):
        items = get_items(top, nick, page, page_size, expire)
        yield items

def crawl_rates(top, items, page_size=40, calllimit=60, expire=600.0):
    for item in waitlimit(calllimit, 60.0, items):
        page = 0
        while True:
            page += 1
            rates = get_rate(top, item[2], item[1], page, page_size)
            if not rates:
                break
            yield rates
"""

if __name__ == '__main__':
    log_init("TaobaoLogger")
    """
    print list(get_taobao_items(get_top(), ["19555209099",]))
    #time.sleep(1)
    print get_promotion_info(get_top(), "23476128281")
    print get_spmeffect_trade(get_taobaoke_top(), '2013-04-22')
    print get_spmeffect_trade(get_taobaoke_top(), '2013-04-23')
    print get_spmeffect_trade(get_taobaoke_top(), '2013-04-22', True, True)
    print get_spmeffect_trade(get_taobaoke_top(), '2013-04-23', True, True)
    #get_taobao_cates(get_taobaoke_top())
    #get_taobao_trade(get_rand_top(), '207801937350421')
    """

    print get_taobao_itemcats(get_top(), "50124001")
