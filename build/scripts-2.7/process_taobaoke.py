# coding: utf-8

import gflags
import datetime
import logging
import re
import sys
import traceback
import time
import random

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.utils import extract_json_from_jsonp
from pygaga.helpers.statsd import Statsd

from guang_crawler.taobao_api import convert_taobaoke_widget

FLAGS = gflags.FLAGS

gflags.DEFINE_enum('action', 'update', ['remove', 'update', 'vip'], "action: remove/update/vip")
gflags.DEFINE_boolean('force', False, "force convert taobaoke")
gflags.DEFINE_boolean('all', False, "remove all taobaoke link")
gflags.DEFINE_list('vipshopids', [4,5,15,111], "client shop ids")
gflags.DEFINE_integer('shop', 0, "remove all taobaoke link for shop")

gflags.DEFINE_integer('limit', 0, "for test: limit how much items proccess")
gflags.DEFINE_string('where', ' true ', "additional where")
gflags.DEFINE_integer('interval', 3, "convert how many days")

gflags.DEFINE_string('pid', '30146700', "default taobaoke pid")

gflags.DEFINE_boolean('dryrun', False, "is in dry run mode?")

logger = logging.getLogger('TaobaokeLogger')

def do_all(fn):
    db = get_db_engine()

    where_sql = " %s" % (FLAGS.where)
    results = db.execute("select id from shop where type < 3 and %s" % where_sql)

    for result in results:
        fn(result[0], db)
        time.sleep(1.0)

def rollback_shop(shop_id, db):
    if not db:
        db = get_db_engine()

    sql = "select id,num_id,null,null from item where shop_id = %s and detail_url like '%%%%s.click.taobao.com%%%%'" % shop_id
    if FLAGS.limit:
        sql += " limit " + str(FLAGS.limit)
    results = db.connect().execute(sql)
    for i, result in enumerate(results):
        new_url = "http://item.taobao.com/item.htm?id=%s" % result[1]
        sql = "update item set detail_url=\'%s\' where id = %s" % (new_url, result[0])
        logger.debug("Run sql %s/%s: %s" % (i, results.rowcount, sql))
        if not FLAGS.dryrun:
            db.execute(sql)

def join_iids(results):
    return ','.join([str(x[1]) for x in results])

def filter_retry_items(results):
    for row in results:
        failed_count = row[2]
        last_time = row[3]
        if last_time is None or failed_count is None or failed_count <= 10 or FLAGS.force:
            yield row
        else:
            rand_wait_seconds = 0
            if failed_count > 10 and failed_count < 40:
                rand_wait_seconds = random.randint(2*86400, 24*86400)
            if failed_count > 40:
                rand_wait_seconds = random.randint(2*86400, 24*86400*3)
            time_diff = datetime.datetime.now() - last_time
            if time_diff.days * 86400 + time_diff.seconds > rand_wait_seconds:
                yield row
            else:
                logger.debug("Skipping %s", row)

def filter_tbk_items(results):
    for row in results:
        re_detail_url = row[4]
        if re_detail_url is None or re_detail_url.find('s.click.taobao.com') < 0:
            yield row
        else:
            logger.debug("Skipping %s", row)

def update_vip_shop(shop_id, db=None):
    if not db:
        db = get_db_engine()

    limitsql = ""
    if FLAGS.limit:
        limitsql += " limit " + str(FLAGS.limit)

    if shop_id:
        shop_str = " shop.id = %s and " % shop_id
    else:
        shop_str = " shop.id in (%s) and " % ','.join(map(str, FLAGS.vipshopids))

    if FLAGS.interval > 0:
        from_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(FLAGS.interval), "%Y-%m-%d %H:%M:%S")
        shop_str += " (created > '%s' or modified > '%s') and " % (from_date, from_date)

    sql = "select item.id,item.num_id,shop.type,item.detail_url,item_re.detail_url from shop,item left join item_re on item.id=item_re.item_id where %s shop.type <= 2 and shop.status = 1 and item.status = 1 and item.shop_id = shop.id" % shop_str
    results = db.connect().execute(sql + limitsql)

    total = results.rowcount
    logger.debug("Processing %s result %s", sql, total)
    if total == 0:
        logger.info("nothing to do with shop %s", shop_str)
        return

    pos = 0
    converted = 0
    SPMRE = re.compile("^(.*spm=)([^&]*)(.*)$")
    for input, outputstr in convert_taobaoke_widget(list(filter_tbk_items(results)), fn_join_iids=join_iids, calllimit=60, outer_code=None, appkey='21315963', appsec='549d623e612832df7720101f83f951b9'):
        if not outputstr:
            logger.debug("Converted failed %s null %s progress %s/%s/%s -> in %s" % (input, shop_id, converted, pos, total, len(input)))
            continue
        output = extract_json_from_jsonp(outputstr)
        pos += len(input)
        if not output:
            logger.debug("Converted failed empty %s %s progress %s/%s/%s -> in %s" % (input, shop_id, converted, pos, total, len(input)))
            continue
        if output['total_results'] == 0 or not output['taobaoke_items']:
            logger.debug("No output %s %s %s/%s/%s", input, shop_id, converted, pos, total)
            continue
        succ_len = len(output['taobaoke_items']['taobaoke_item'])
        logger.info("Converted shop %s progress %s/%s/%s -> in %s out %s %s" % (shop_id, converted, pos, total, len(input), output['total_results'], succ_len))
        converted += succ_len
        Statsd.update_stats('guang.taobaoapi.convert', delta=succ_len)
        try:
            numid2id = dict([(int(num_id), id) for id, num_id, shop_type, jn_url, re_url in input])
            for result in output['taobaoke_items']['taobaoke_item']:
                isql = ""
                try:
                    num_iid = result['num_iid']
                    click_url = result['click_url'] + "&u=re_UCTRAC_CLK_&unid=re_UCTRAC_CLK_"
                    # conver spm to xtao
                    if click_url.find('spm=') > 0:
                        click_url = SPMRE.subn(r'\g<1>2014.21315963.1.0\g<3>', click_url)[0]
                    else:
                        click_url += '&spm=2014.21315963.1.0'
                    id = numid2id[num_iid]
                    isql = "insert into item_re (item_id, detail_url) values (%s, '%s') on duplicate key update detail_url='%s'" % (id, click_url, click_url)
                    logger.debug("process %s %s/%s -> %s", shop_id, pos, total, isql)
                    if not FLAGS.dryrun:
                        db.execute(isql.replace('%', '%%'))
                except KeyboardInterrupt:
                    raise
                except Exception, e:
                    logger.debug("in %s out %s" % (numid2id, result))
                    logger.warn("convert failed %s %s" % (isql, traceback.format_exc()))
        except KeyboardInterrupt:
            raise
        except:
            logger.warn("process failed %s %s reason %s" % (input, output, traceback.format_exc()))
    logger.info("Convert result %s - %s", converted, total)

    # retry sql
    results = db.connect().execute(sql + limitsql)
    for row in filter_tbk_items(results):
        id, num_id, shop_type, jn_url, re_url = row
        if not re_url:
            sql = "insert into item_re (item_id, detail_url) values (%s, '%s')" % (id, 'http://item.taobao.com/item.htm?id=%s&spm=2014.21315963.1.0' % num_id)
            db.execute(sql)

def update_shop(shop_id, db):
    if not db:
        db = get_db_engine()

    tbk = list(db.execute("select * from tbk where shop_id=%s" % shop_id))
    if tbk:
        tbk_pid = str(tbk[0][1])
    else:
        tbk_pid = FLAGS.pid

    limitsql = ""
    if FLAGS.limit:
        limitsql += " limit " + str(FLAGS.limit)

    if shop_id:
        shop_str = " shop.id = %s and " % shop_id
    else:
        shop_str = ""

    if FLAGS.interval > 0:
        from_date = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(FLAGS.interval), "%Y-%m-%d %H:%M:%S")
        shop_str += " (created > '%s' or modified > '%s') and " % (from_date, from_date)

    if not FLAGS.force:
        sql = "select item.id,item.num_id,tbk_item_convert.failed_count,tbk_item_convert.last_time from shop,item left join tbk_item_convert on tbk_item_convert.item_id=item.id where %s shop.type <= 2 and shop.status = 1 and item.status = 1 and item.shop_id = shop.id and item.detail_url not like '%%%%s.click.taobao.com%%%%'" % shop_str
        results = db.connect().execute(sql + limitsql)
    else:
        sql = "select item.id,item.num_id,tbk_item_convert.failed_count,tbk_item_convert.last_time from shop,item left join tbk_item_convert on tbk_item_convert.item_id=item.id where %s shop.type <= 2 and shop.status = 1 and item.status = 1 and item.shop_id = shop.id" % shop_str
        results = db.connect().execute(sql + limitsql)

    total = results.rowcount
    if total == 0:
        logger.info("nothing to do with shop %s", shop_id)
        return

    pos = 0
    converted = 0
    SPMRE = re.compile("^(.*spm=)([^&]*)(.*)$")
    for input, outputstr in convert_taobaoke_widget(list(filter_retry_items(results)), fn_join_iids=join_iids, calllimit=60, outer_code=None):
        if not outputstr:
            logger.debug("Converted failed null %s progress %s/%s/%s -> in %s" % (shop_id, converted, pos, total, len(input)))
            continue
        output = extract_json_from_jsonp(outputstr)
        pos += len(input)
        if not output:
            logger.debug("Converted failed empty %s progress %s/%s/%s -> in %s" % (shop_id, converted, pos, total, len(input)))
            continue
        if output['total_results'] == 0 or not output['taobaoke_items']:
            logger.debug("No output %s %s/%s/%s", shop_id, converted, pos, total)
            for row in input:
                if not FLAGS.dryrun:
                    db.execute("insert into tbk_item_convert(item_id, failed_count, last_time) values(%s, 1, now()) on duplicate key update failed_count=failed_count+1, last_time=now()" % row[0])
            continue
        succ_len = len(output['taobaoke_items']['taobaoke_item'])
        logger.info("Converted shop %s progress %s/%s/%s -> in %s out %s %s" % (shop_id, converted, pos, total, len(input), output['total_results'], succ_len))
        converted += succ_len
        Statsd.update_stats('guang.taobaoapi.convert', delta=succ_len)
        try:
            numid2id = dict([(int(num_id), id) for id, num_id, failed_count, last_time in input])
            for result in output['taobaoke_items']['taobaoke_item']:
                sql = ""
                try:
                    num_iid = result['num_iid']
                    click_url = result['click_url'] + "&u=jn_UCTRAC_CLK_&unid=jn_UCTRAC_CLK_"
                    # conver spm to xtao
                    if click_url.find('spm=') > 0:
                        click_url = SPMRE.subn(r'\g<1>2014.12669715.1.0\g<3>', click_url)[0]
                    else:
                        click_url += '&spm=2014.12669715.1.0'
                    id = numid2id[num_iid]
                    sql = "update item set detail_url='%s' where id=%s" % (click_url, id)
                    logger.debug("process %s %s/%s -> %s", shop_id, pos, total, sql)
                    if not FLAGS.dryrun:
                        db.execute(sql.replace('%', '%%'))
                        db.execute("delete from tbk_item_convert where item_id=%s" % id)
                except KeyboardInterrupt:
                    raise
                except Exception, e:
                    logger.debug("in %s out %s" % (numid2id, result))
                    logger.warn("convert failed %s %s" % (sql, traceback.format_exc()))
        except KeyboardInterrupt:
            raise
        except:
            logger.warn("process failed %s %s reason %s" % (input, output, traceback.format_exc()))
    logger.info("Convert result %s - %s", converted, total)

if __name__ == "__main__":
    log_init(['TaobaokeLogger', 'TaobaoLogger'], "sqlalchemy.*")
    if FLAGS.action == 'remove':
        if FLAGS.all:
            do_all(rollback_shop)
        else:
            rollback_shop(FLAGS.shop, None)
    elif FLAGS.action == 'update':
        if FLAGS.all:
            do_all(update_shop)
        else:
            update_shop(FLAGS.shop, None)
    elif FLAGS.action == 'vip':
        update_vip_shop(FLAGS.shop)

