#!/usr/bin/env python
# coding: utf-8

#import warnings
#warnings.filterwarnings('error', category=MySQLdb.Warning)
import logging

import csv
import gflags
import traceback
from datetime import datetime, timedelta

from pygaga.helpers.logger import log_init
from pygaga.helpers.ratelimit import waitlimit
from pygaga.helpers.dbutils import get_db_engine

from guang_crawler.taobao_api import get_report, get_top

gflags.DEFINE_string('sessionid', "6101a2091623b5bc1ec813739d8d158539f719c08ef9a96814424732", "taobao session id")
gflags.DEFINE_string('start', "", "get report start from yyyy-mm-dd, default today")
gflags.DEFINE_string('end', "", "get report end to yyyy-mm-dd, default today")
gflags.DEFINE_integer('interval', 0, "if not 0, ignore start params, start=end-interval")
gflags.DEFINE_integer('limit', 30, "how many api calls per minutes")

gflags.DEFINE_boolean('dryrun', False, "not commit to database")

gflags.DEFINE_boolean('csv', False, "output to csv")
gflags.DEFINE_string('csv_encoding', 'gb18030', "csv encoding, gbk or utf8")
gflags.DEFINE_string('csv_split', ';', "csv splitter")
gflags.DEFINE_string('csv_quote', '"', "csv quote")
gflags.DEFINE_string('csv_filename', 'output.csv', "csv filename")

logger = logging.getLogger('TaobaoLogger')

FLAGS = gflags.FLAGS

def dates():
    end = datetime.now()
    interval = 1
    if FLAGS.end != "":
        end = datetime.strptime(FLAGS.end, "%Y-%m-%d %H:%M:%S")
    if FLAGS.interval != 0:
        interval = FLAGS.interval
    elif FLAGS.start != "":
        interval = (end - datetime.strptime(FLAGS.start, "%Y-%m-%d %H:%M:%S")).days
    for i in range(interval, 0, -1):
        yield datetime.strftime(end-timedelta(i-1), "%Y-%m-%d %H:%M:%S")

def writecsv(writer, row, encoding=FLAGS.csv_encoding):
    writer.writerow([unicode(s).encode(encoding) for s in row])

def main():
    if FLAGS.sessionid == "":
        logger.error("Get SESSION from http://container.api.taobao.com/container?appkey=12525923")
    db = None
    csv_w = None
    if not FLAGS.dryrun:
        db = get_db_engine()

    if FLAGS.csv:
        csv_w = csv.writer(open(FLAGS.csv_filename, "wb"), delimiter=FLAGS.csv_split,
            quotechar=FLAGS.csv_quote, quoting=csv.QUOTE_NONNUMERIC)
        csv_w.writerow(["report_date", "outer_code", "commission_rate", "item_title", "seller_nick", "num_iid",
                        "shop_title", "app_key", "commission", "trade_id", "pay_time", "item_num",
                        "category_id", "pay_price", "real_pay_fee", "category_name"])
    for d in waitlimit(FLAGS.limit, 60.0, dates()):
        logger.info("Fetching %s %s", d, FLAGS.sessionid)
        try:
            pageno = 1
            total = 100
            result_len = 100
            got = 0
            while result_len >= total:
                report = get_report(get_top(), d, FLAGS.sessionid, pageno, total)
                if not report:
                    logger.info("result %s %s null", d, pageno)
                    break
                else:
                    logger.info("result %s %s", d, pageno)
                result_len = len(report['taobaoke_payments']['taobaoke_payment'])
                got += result_len
                logger.info("result %s %s -> %s %s", d, pageno, got, len(report['taobaoke_payments']['taobaoke_payment']))
                if result_len > 0:
                    members = report['taobaoke_payments']['taobaoke_payment']
                    for m in members:
                        try:
                            #import pdb; pdb.set_trace()
                            check_sql = """select outer_code, commission_rate, item_title, seller_nick, num_iid,
                                shop_title, app_key, commission, trade_id, pay_time, item_num,
                                category_id, pay_price, real_pay_fee, category_name, create_time,
                                confirm_time, status from taobao_report
                                where trade_id=%s""" % m['trade_id']
                            result = list(db.execute(check_sql))
                            if result:
                                if result[0][0] == m.get('outer_code', '') and result[0][4] == m['num_iid']:
                                    logger.debug("already exists in db, skip %s vs %s", result[0], m)
                                else:
                                    logger.warn("same trade id, something wrong! %s %s" % (m, result))
                                continue
                            sql = """insert into taobao_report (outer_code, commission_rate, item_title, seller_nick,
                                num_iid, shop_title, app_key, commission, trade_id, pay_time, item_num,
                                category_id, pay_price, real_pay_fee, category_name, create_time) values (
                                "%s", "%s", "%s", "%s", %s, "%s", "%s", "%s", %s, "%s", %s, %s, "%s", "%s", "%s", now()
                                )""" % (
                                m.get('outer_code', ''), m['commission_rate'].replace('%', '%%'), m['item_title'].replace('%', '%%'),
                                m['seller_nick'].replace('%', '%%'), m['num_iid'],
                                m['shop_title'].replace('%', '%%'), m['app_key'], m['commission'], m['trade_id'], m['pay_time'], m['item_num'],
                                m['category_id'], m['pay_price'], m['real_pay_fee'], m.get('category_name','').replace('%', '%%')
                                )
                            logger.debug(sql)
                            if db:
                                try:
                                    db.execute(sql)
                                except:
                                    logger.warn("insert failed sql %s --> err %s", sql, traceback.format_exc())
                            if csv_w:
                                writecsv(csv_w, [d, m.get('outer_code', ''), m['commission_rate'], m['item_title'], m['seller_nick'], m['num_iid'],
                                    m['shop_title'], m['app_key'], m['commission'], m['trade_id'], m['pay_time'], m['item_num'],
                                    m['category_id'], m['pay_price'], m['real_pay_fee'], m.get('category_name', '')])
                        except:
                            logger.error("Got error %s %s", m, traceback.format_exc())
                pageno += 1
        except:
            logger.error("Got fatal error %s %s", d, traceback.format_exc())

if __name__ == "__main__":
    log_init("TaobaoLogger", "sqlalchemy.*")
    main()
