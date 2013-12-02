#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
#import time

#from pandas import Series

from pygaga.helpers.logger import log_init
from pygaga.helpers.dateutils import datestr
from pygaga.helpers.dbutils import get_db_engine
#from pygaga.model.feature import numberic2SignalFn

FLAGS = gflags.FLAGS

logger = logging.getLogger('GuangLogger')

### click --> pay match
class ClickPayProcessor:
    def __init__(self):
        self.guangdb = get_db_engine()

    def process(self):
        where = "click_time >= '%s' and click_time <= '%s'" % (datestr(FLAGS.start), datestr(FLAGS.end))
        click_sql = "select id, outer_code, item_id from click_item_log where %s" % where
        logger.debug("Executing %s", click_sql)
        click_items = list(self.guangdb.execute(click_sql))
        logger.debug("processing %s", len(click_items))
        for click_item in click_items:
            outer_code = 'jn%s' % click_item[1]
            pay_sql = "select id, num_iid, pay_time, trade_id, item_title, seller_nick, shop_title from taobao_report where outer_code='%s'" % outer_code
            pay_item = list(self.guangdb.execute(pay_sql))
            # price, volume, votescore, votescore_s2, created
            if pay_item: # positive
                logger.debug("Matched logid %s reportid %s", click_item[0], pay_item[0][0])
                self.guangdb.execute("update click_item_log set taobao_report_id=%s where id=%s" % (pay_item[0][0], click_item[0]))

def click_pay_main():
    logProcessor = ClickPayProcessor()
    logProcessor.process()
    logger.info("Results shop matched")

if __name__ == "__main__":
    log_init(["GuangLogger","urlutils"], "sqlalchemy.*")
    click_pay_main()
