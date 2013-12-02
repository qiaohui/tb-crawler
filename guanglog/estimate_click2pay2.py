#!/usr/bin/env python
# coding: utf-8

#import csv
#import traceback
import gflags
import logging
#import simplejson
from collections import namedtuple

from pandas import Series

from pygaga.helpers.logger import log_init
from pygaga.model.plotroc import gnuplot
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.dateutils import datestr
from pygaga.model.feature import numberic2SignalFn

gflags.DEFINE_integer('limit', 0, 'limit counts')
gflags.DEFINE_string('plot_output', 'scale.ps', 'input file name')
gflags.DEFINE_string('scale_output', 'scale.txt', 'input file name')

FLAGS = gflags.FLAGS

logger = logging.getLogger('GuangLogger')

class RangeSplit:
    def __init__(self, range_tuples=[]):
        ranges = [range(r[0], r[1], r[2]) for r in range_tuples]
        self.range_sum = sum(ranges, [])
        range_headers = []
        for r, rt in zip(ranges, range_tuples):
            range_headers.append(["num%s-num%s" % (x, x+rt[2]) for x in r])
        self.headers = sum(range_headers, [])

def est_main():
    numid2volumeprice = {}
    click_items, paid_items = load_click_items(numid2volumeprice)
    pay_items = load_pay_items(paid_items)
    volume_splitter = RangeSplit([(0,20,1),(20,100,10),(100,400,50),(400,1000,100)])
    price_splitter = RangeSplit([(0,100,10),(100,400,50),(400,800,100)])
    stats_dist_diff(click_items, pay_items, volume_splitter, 'volume', numid2volumeprice)
    stats_dist_diff(click_items, pay_items, price_splitter, 'price', numid2volumeprice)

def stats_dist_diff(click_items, pay_items, splitter, key, numid2volumeprice):
    def_item = {'volume':-1, 'price':-1}
    fn_conv = lambda x:numid2volumeprice.get(long(x.num_id), def_item)[key]

    click_sig2prob = stats_items_dist(click_items, fn_conv, 'Click', splitter)
    pay_sig2prob = stats_items_dist(pay_items, fn_conv, 'Pay', splitter)
    #import pdb; pdb.set_trace()

    header = splitter.headers

    sig2header = dict(zip(range(1,len(header)+1), header))
    scales = []
    scale_f = open("%s_%s" % (key, FLAGS.scale_output), "w")
    #xindex = range(21) + range(30, 110, 10) + range(150,350,50) + range(400,1200,200)
    xindex = splitter.range_sum
    for i in range(1, len(splitter.headers)+1):
        name = sig2header[i]
        if pay_sig2prob.get(i, -1) != -1 and click_sig2prob.get(i, -1) != -1:
            v = pay_sig2prob.get(i, -1) / click_sig2prob.get(i, -1)
        else:
            v = 0
        #scales.append((i, v))
        scales.append((xindex[i-1], v))
        scale_f.write("%s : %s : %s : %s\n" % (name, click_sig2prob.get(i, -1), pay_sig2prob.get(i, -1), v))
    g = gnuplot("%s_%s" % (key, FLAGS.plot_output))
    g.xlabel = 'signal'
    g.ylabel = 'scale'
    dataset = {0:{'xy_arr':scales}}
    g.plotline(dataset)

def stats_items_dist(iter, fn_conv, name, splitter):
    logger.info("Stating %s", name)
    n2s = numberic2SignalFn(int, splitter.range_sum)

    volumes = []
    for i in iter:
        iv = fn_conv(i)
        s = n2s(iv)
        volumes.append(s)

    cs = Series(volumes)
    g = cs.groupby(cs.values).agg(len)
    total = len(volumes)
    sig2prob = {}
    for sig, count in g.iteritems():
        if sig == 0:
            logger.warn("%s sig %s -- %s %s %s", name, sig, count*1.0/total, count, total)
            total -= count
        else:
            logger.info("%s sig %s -- %s %s %s", name, sig, count*1.0/total, count, total)
            sig2prob[sig] = count*1.0/total
    return sig2prob

def load_click_items(numid2volumeprice):
    logger.info("Loading click items")
    click_items = []
    paid_items = []
    click_item_type = namedtuple("ClickItemType", 'click_hash item_id click_time click_ip area_code click_price click_volume item_price item_volume shop_nick taobao_report_id num_id')

    db = get_db_engine()
    where = "click_time>='%s' and click_time<='%s'" % (datestr(FLAGS.start), datestr(FLAGS.end))
    if FLAGS.limit > 0:
        where += " limit %s" % FLAGS.limit
    sql = "select outer_code,item_id,click_time,click_ip,click_area,click_price,click_volume,item.price,item.volume,shop.nick,click_item_log.taobao_report_id,item.num_id from click_item_log left join item on click_item_log.item_id=item.id left join shop on shop.id=item.shop_id where %s" % where
    logger.debug("fetching %s", sql)
    results = db.execute(sql)
    progress = 0
    item_matched = 0
    logger.info("Processing click items %s", results.rowcount)
    price_diffs = 0
    for line in results:
        progress += 1
        click_item = click_item_type(*line)
        if not click_item.num_id:
            logger.warn("no numid %s", click_item)
            continue
        click_items.append(click_item)
        if click_item.item_id > 0:
            item_matched += 1
        volume = click_item.item_volume
        if not volume or volume == 0:
            logger.warn("item %s abnormal %s", click_item.item_id, volume)
            volume = 0.2
        elif volume > 800:
            volume = 800

        price = click_item.click_price
        if click_item.item_price and price > click_item.item_price * 1.5:
            price = click_item.item_price
            price_diffs += 1
            logger.warn("Price diff paid? %s %s/%s too much %s - %s", click_item.taobao_report_id, price_diffs, results.rowcount, click_item.click_price, click_item.item_price)
        if price > 500.0:
            price = 500.0
        if not price or price < 0.5:
            logger.warn("price %s abnormal %s", click_item.item_id, price)
            price = 1.0

        numid2volumeprice[long(click_item.num_id)] = {'volume' : volume, 'price' : price}
        if click_item.taobao_report_id:
            paid_items.append(click_item.taobao_report_id)
    logger.info("Total click %s item matched %s", len(click_items), item_matched)
    return click_items, paid_items

def load_pay_items(paid_items):
    logger.info("Loading pay items")
    pay_item_type = namedtuple('PayItemType', 'created name num_id shop_id shop_name count price total_price comm_rate total_comm status order_id')

    pay_items = []
    db = get_db_engine()
    for id in paid_items:
        results = db.execute("select create_time,item_title,num_iid,shop.id,shop.taobao_title,item_num,real_pay_fee,pay_price,commission_rate,commission,taobao_report.status,trade_id from taobao_report,item,shop where item.shop_id=shop.id and cast(taobao_report.num_iid as char)=item.num_id and taobao_report.id=%s" % id)
        if results.rowcount:
            line = list(results)[0]
            #logger.debug("loaded pay items %s %s", id, line)
            pay_item = pay_item_type(*line)
            pay_items.append(pay_item)
        else:
            logger.warn("not faound taobaoreport %s", id)
    return pay_items

if __name__ == '__main__':
    log_init(["GuangLogger","urlutils"], "sqlalchemy.*")
    est_main()
