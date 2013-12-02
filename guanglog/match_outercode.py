#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
import time

from pandas import Series

from pygaga.helpers.logger import log_init
from pygaga.helpers.dateutils import date2ts
from pygaga.helpers.dbutils import get_db_engines
from pygaga.model.feature import numberic2SignalFn

gflags.DEFINE_list('xdbconnstrs', ["guang:guang@192.168.32.10:3306/guang?charset=utf8","stat:stat@192.168.32.178:3306/stat?charset=utf8"], "database connstrs")
gflags.DEFINE_string('out_file', 'sample.txt', 'output file name')
gflags.DEFINE_boolean("pay2clk", True, 'convert pay to click? or click to pay?')

FLAGS = gflags.FLAGS

logger = logging.getLogger('GuangLogger')

### click --> pay match
class ClickPayProcessor:
    def __init__(self):
        self.total = 0
        self.matched = 0
        self.notfound = 0
        dbs = get_db_engines(**{'dbconnstrs' : FLAGS.xdbconnstrs})
        self.guangdb = dbs[0]
        self.statdb = dbs[1]

    def process(self):
        where = "click_time >= %s and click_time < %s" % (time.mktime(FLAGS.start.timetuple()), time.mktime(FLAGS.end.timetuple()))
        click_sql = "select click_hash, creative_id, media_id, click_ip, click_time from conversion where %s" % where
        logger.debug("Executing %s", click_sql)
        click_items = list(self.statdb.execute(click_sql))
        samples = []
        for click_item in click_items:
            self.total += 1
            outer_code = 'jn%s' % click_item[0]
            creative_id = click_item[1]
            pay_sql = "select num_iid, pay_time, trade_id, item_title, seller_nick, shop_title from taobao_report where outer_code='%s'" % outer_code
            pay_item = list(self.guangdb.execute(pay_sql))
            item_sql = "select item.id, num_id, price, volume, votescore, votescore_s2, created, title, category, shop.name, shop.nick from item,shop where uctrac_creative_id=%s and item.shop_id=shop.id;" % creative_id
            item = list(self.guangdb.execute(item_sql))
            if not item:
                self.notfound += 1
                logger.warn("Item not matched creativeid %s %s-%s-%s/%s", creative_id, self.notfound, self.matched, self.total, len(click_items))
            else:
                # price, volume, votescore, votescore_s2, created
                if pay_item: # positive
                    self.matched += 1
                    samples.append((item[0][2], item[0][3], item[0][4], item[0][5], date2ts(item[0][6]), 1))
                else: # negative
                    samples.append((item[0][2], item[0][3], item[0][4], item[0][5], date2ts(item[0][6]), 0))
        # write to files
        f = open(FLAGS.out_file, "w")
        f.write("price, volume, score, lctr, createts, y\n")
        for sample in samples:
            f.write("%s,%s,%s,%s,%s,%s\n" % (sample[0], sample[1], sample[2], sample[3], sample[4], sample[5]))
        f.close()

### pay --> click match
class PayClickProcessor:
    def __init__(self):
        self.total = 0
        self.matched = 0
        self.shop_matched = 0
        self.item_matched = 0
        self.notmatched_item_exists = 0
        dbs = get_db_engines(**{'dbconnstrs' : FLAGS.xdbconnstrs})
        self.guangdb = dbs[0]
        self.statdb = dbs[1]
        self.timediffs = []
        self.pricediffs = []
        self.volumediffs = []
        self.volumesignal_diffs = []
        self.volume2signal = numberic2SignalFn(int, [1, 10, 100, 400])

    def compare(self, pay_item, click_item, click_stat_item):
        pay_shop = pay_item[5]
        click_shop = click_item[5]
        pay_title = pay_item[4]
        click_title = click_item[2]
        pay_time = pay_item[2]
        click_time = click_stat_item[3]
        pay_numid = pay_item[1]
        click_numid = click_item[1]
        click_price = click_item[7]
        click_volume = click_item[6]
        tdiff = time.mktime(pay_time.timetuple()) - click_time
        self.timediffs.append(tdiff)
        #import pdb; pdb.set_trace()
        if pay_numid == long(click_numid):
            self.item_matched += 1
        else:
            # check if pay-numid exists?
            matched_payitems = self.guangdb.execute("select volume, price from item where num_id='%s'" % pay_numid)
            if matched_payitems.rowcount > 0:
                self.notmatched_item_exists += 1
                pay_volume, pay_price = list(matched_payitems)[0]
                self.pricediffs.append(abs(pay_price - click_price))
                self.volumediffs.append(abs(click_volume - pay_volume))
                pay_vs = self.volume2signal(pay_volume)
                click_vs = self.volume2signal(click_volume)
                if pay_vs != click_vs:
                    logger.warn("volume signal diff %s, %s", pay_vs, click_vs)
        if pay_shop == click_shop:
            self.shop_matched += 1
        else:
            #print pay_item, "---", click_item
            logger.debug("%s :: %s ||| %s :: %s", pay_shop.encode('utf8'),  click_shop.encode('utf8'), pay_title.encode('utf8'), click_title.encode('utf8'))

    def process(self):
        where = "pay_time >= '%s' and pay_time < '%s'" % (FLAGS.start, FLAGS.end)
        pay_sql = "select outer_code, num_iid, pay_time, trade_id, item_title, seller_nick, shop_title from taobao_report where %s" % where
        logger.debug("Quering %s", pay_sql)
        pay_items = self.guangdb.execute(pay_sql)
        for item in pay_items:
            outer_code = item[0]
            self.total += 1
            if outer_code[:2] == 'jn':
                click_hash = outer_code[2:]
                stat_sql = "select creative_id, media_id, click_ip, click_time from conversion where click_hash='%s'" % click_hash
                stat_item = list(self.statdb.execute(stat_sql))
                if not stat_item:
                    logger.warn("Click hash not found in stat %s", outer_code)
                else:
                    item_sql = "select item.id, num_id, title, category, shop.name, shop.nick, item.volume, item.price from item,shop where uctrac_creative_id=%s and item.shop_id=shop.id;" % stat_item[0][0]
                    item_info = list(self.guangdb.execute(item_sql))
                    if not item_info:
                        logger.warn("Creative not found in guang %s - %s", outer_code, stat_item)
                    else:
                        self.matched += 1
                        self.compare(item, item_info[0], stat_item[0])
            else:
                logger.warn("outer code mismatch %s", outer_code)

def pay_click_main():
    logProcessor = PayClickProcessor()
    logProcessor.process()
    logger.info("Time summary : %s", Series(logProcessor.timediffs).describe())
    logger.info("Volumediff summary : %s", Series(logProcessor.volumediffs).describe())
    logger.info("Pricediff summary : %s", Series(logProcessor.pricediffs).describe())
    logger.info("Results shop matched item %s(%s) shop %s total %s/%s", logProcessor.item_matched, logProcessor.notmatched_item_exists, logProcessor.shop_matched, logProcessor.matched, logProcessor.total)

def click_pay_main():
    logProcessor = ClickPayProcessor()
    logProcessor.process()
    logger.info("Results shop matched %s/%s", logProcessor.matched, logProcessor.total)

if __name__ == "__main__":
    log_init(["GuangLogger","urlutils"], "sqlalchemy.*")
    if FLAGS.pay2clk:
        pay_click_main()
    else:
        click_pay_main()
