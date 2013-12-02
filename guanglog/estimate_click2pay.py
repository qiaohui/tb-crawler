#!/usr/bin/env python
# coding: utf-8

import csv
#import traceback
import gflags
import logging
import simplejson
from collections import namedtuple

from pandas import Series

from pygaga.helpers.logger import log_init
from pygaga.model.plotroc import gnuplot
from pygaga.helpers.dbutils import get_db_engine
from pygaga.model.feature import numberic2SignalFn

gflags.DEFINE_string('pay_input', 'result.csv', 'input file name')
gflags.DEFINE_string('click_input', 'clicklog.20130123.txt', 'input file name')
gflags.DEFINE_string('plot_output', 'scale.ps', 'input file name')
gflags.DEFINE_string('scale_output', 'scale.txt', 'input file name')

FLAGS = gflags.FLAGS

logger = logging.getLogger('GuangLogger')

def est_main():
    numid2volume = {}
    click_items = load_click_items(numid2volume)
    pay_items = load_pay_items()
    click_sig2prob = stats_items(click_items, lambda x:x.item_volume, 'Click')
    #import pdb; pdb.set_trace()
    pay_sig2prob = stats_items(pay_items, lambda x:numid2volume.get(long(x.num_id), -1), 'Pay')

    header = ['num%s'%x for x in range(21)] + ["num%s-%s"%(x-9, x) for x in range(30,110,10)] + ["num%s-%s"%(x-49,x) for x in range(150,350,50)] + ['num%s-num%s'%(x-199,x) for x in range(400,1200,200)]
    sig2header = dict(zip(range(1,len(header)+1), header))
    scales = []
    scale_f = open(FLAGS.scale_output, "w")
    xindex = range(21) + range(30, 110, 10) + range(150,350,50) + range(400,1200,200)
    for i in range(1, 38):
        name = sig2header[i]
        if pay_sig2prob.get(i, -1) != -1 and click_sig2prob.get(i, -1) != -1:
            v = pay_sig2prob.get(i, -1) / click_sig2prob.get(i, -1)
        else:
            v = 0
        #scales.append((i, v))
        scales.append((xindex[i-1], v))
        scale_f.write("%s : %s : %s : %s\n" % (name, click_sig2prob.get(i, -1), pay_sig2prob.get(i, -1), v))
    g = gnuplot(FLAGS.plot_output)
    g.xlabel = 'signal'
    g.ylabel = 'scale'
    dataset = {0:{'xy_arr':scales}}
    g.plotline(dataset)

def stats_items(iter, fn_conv, name):
    logger.info("Stating %s", name)
    #n2s = numberic2SignalFn(int, [0, 1, 10, 100, 400])
    n2s = numberic2SignalFn(int, range(21) + range(30, 110, 10) + range(150,350,50) + range(400,1200,200))

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

def load_click_items(numid2volume):
    logger.info("Loading click items")
    db = get_db_engine()
    json_file = open(FLAGS.click_input)
    click_json = simplejson.load(json_file)
    click_item_type = namedtuple("ClickItemType", 'click_hash source media_id holder_id site admember_id campaign_id adgroup_id creative_id click_time click_ip area_code lpid price pubcat_list user_attr_list score item_price item_volume')
    click_items = []
    creative_matched = 0
    outercode_matched = 0
    progress = 0
    creative2item_cache = {}
    logger.info("Processing click items")
    for line in click_json:
        progress += 1
        click_item = click_item_type(*line)
        click_items.append(click_item)
        if creative2item_cache.has_key(click_item.creative_id):
            rr = creative2item_cache[click_item.creative_id]
        else:
            # creative_id --> (num_id, shop_name) item_price, item_volume
            r = db.execute("select num_id, shop.nick from item,shop where item.shop_id=shop.id and item.uctrac_creative_id=%s" % click_item.creative_id)
            if not r.rowcount:
                logger.warn("creative not matched %s %s/%s", click_item.creative_id, progress, len(click_json))
                continue
            rr = creative2item_cache[click_item.creative_id] = list(r)
        creative_matched += 1
        num_id, seller_nick = rr[0]
        #import pdb; pdb.set_trace()
        numid2volume[long(num_id)] = click_item.item_volume
        click_hash = 'jn%s' % click_item.click_hash
        r2 = db.execute('select 1 from taobao_report where outer_code="%s"' % click_hash)
        if r2.rowcount:
            outercode_matched += 1
    logger.info("Total click %s creative matched %s outercode matched %s", len(click_items), creative_matched, outercode_matched)
    return click_items

def load_pay_items():
    logger.info("Loading pay items")
    db = get_db_engine()
    csv_file = open(FLAGS.pay_input)
    csv_reader = csv.reader(csv_file)
    header = csv_reader.next()
    pay_item_type = namedtuple('PayItemType', 'created name num_id shop_id shop_name count price total_price comm_rate comm tmall_rate tmall_comm total_comm status order_id')
    pay_items = []
    order_matched = 0
    for line in csv_reader:
        pay_item = pay_item_type(*line)
        pay_items.append(pay_item)
        r = db.execute("select 1 from taobao_report where trade_id=%s" % pay_item.order_id)
        if r.rowcount:
            order_matched += 1
    logger.info("Total payed %s order matched %s", len(pay_items), order_matched)
    return pay_items

if __name__ == '__main__':
    log_init(["GuangLogger","urlutils"], "sqlalchemy.*")
    est_main()
