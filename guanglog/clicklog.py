#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
import simplejson
import traceback
from glob import glob
from collections import namedtuple

from pygaga.helpers.logger import log_init
from pygaga.helpers.ip import IP2Area
from pygaga.helpers.dateutils import eachday, datestr
from pygaga.helpers.dbutils import get_db_engine

from pygaga.log_decoder import decode_click
from pygaga.log_decoder import decode_click_ex

gflags.DEFINE_string('out_file', 'clicklog.txt', 'output file name')
gflags.DEFINE_boolean('commit', False, 'is commit to database')
gflags.DEFINE_boolean('debug_log', False, 'is debug click log')

FLAGS = gflags.FLAGS

logger = logging.getLogger('GuangLogger')

ip2area = IP2Area()

def get_click(line):
    try:
        fields       = line[:-1].split(" ")
        machine, click_ex_msg, click_msg, score, why = fields[:5]
        score        = int(score)
        click_ex_obj = decode_click_ex(click_ex_msg)
        click_obj    = decode_click(click_msg)
    except Exception:
        logger.warn("Parse click line failed %s - %s", line, traceback.format_exc())
        return
    return (click_obj, click_ex_obj, score, why)

def get_record(click):
    if not click:
        return
    click_obj, click_ex_obj, score, why = click
    click_hash = ("%x" % click_ex_obj.click_hash)
    source = "uctrac"
    media_id   = click_obj.display_info.media_id
    holder_id   = click_obj.display_info.holder_id
    site  = ""
    admember_id = click_obj.ad_info.admember_id
    campaign_id = click_obj.ad_info.campaign_id
    adgroup_id  = click_obj.ad_info.adgroup_id
    creative_id = click_obj.ad_info.creative_id
    click_time = click_ex_obj.click_time
    click_ip   = click_ex_obj.click_ip
    area_code = ip2area.ipint2code(click_ip)
    lpid      = 0
    price     = click_obj.ad_info.price / 1000000.0

    if media_id == 10167:
        source = "google_exchange"
    if media_id == 10158:
        source = "tanx"
    if media_id != 10140: # not guang
        return
    source = "guang"

    if FLAGS.debug_log:
        import pdb; pdb.set_trace()

    item_price = click_obj.ad_info.guang_info.price
    item_volume = click_obj.ad_info.guang_info.volume
    #item_price = click_obj.ad_info.ectr
    #item_volume = click_obj.ad_info.level

    item_id = click_obj.ad_info.guang_info.item_id
    shop_id = click_obj.ad_info.guang_info.item_id
    user_id = click_obj.ad_info.guang_info.user_id
    user_xxid = click_ex_obj.user_info.xxid or "M" + click_ex_obj.user_info.muid

    #get pub_cates.
    pubcat_list = click_obj.backend_info.publisher_category
    pubcat_list = [(i.id, i.weight) for i in pubcat_list ]
    pubcat_list.sort(lambda x,y: 1 if x[1] < y[1] else -1)
    if len(pubcat_list) > 5:
        pubcat_list = pubcat_list[:5]
    pubcat_list = "|".join([ ("%d=%.4f" % i) for i in pubcat_list])

    #get user_attr_list
    user_attr_list = "|".join(map(str, click_obj.user_attr_list))
    if len(user_attr_list) > 127:
        tmp = user_attr_list.rfind('|', 0, 127)
        user_attr_list =  user_attr_list[0:tmp] if tmp != -1 else ""

    return (click_hash, source, media_id, holder_id, site, admember_id, campaign_id, adgroup_id, creative_id, click_time, click_ip, area_code, lpid, price, pubcat_list, user_attr_list, score, item_price, item_volume, item_id, shop_id, user_id, user_xxid)

def generate_load_sql():
    return "SELECT click_hash FROM conversion WHERE click_time BETWEEN UNIX_TIMESTAMP(%s) - 7200 AND UNIX_TIMESTAMP(%s) + 93600" % (FLAGS.start, FLAGS.end)

def generate_insert_sql(data):
    #uctrac_clk, media, site, view_time, view_ip, area, prov, grpid, kwid = tp
    INSERT_SQL = "INSERT INTO conversion(`click_hash`, `source`, `media_id`, `holder_id`, `site`, `admember_id`, `campaign_id`, `group_id`, `creative_id`, `click_time`, `click_ip`, `click_area`, `lpid`, `click_price`, `pub_cates`, `user_attrs`, `click_score`) \
                  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE click_price = VALUES(click_price), pub_cates = VALUES(pub_cates), user_attrs = VALUES(user_attrs)"
    #cursor.executemany(INSERT_SQL, data)
    return INSERT_SQL

def insert_match(db, record):
    click_item_type = namedtuple("ClickItemType", 'click_hash source media_id holder_id site admember_id campaign_id adgroup_id creative_id click_time click_ip area_code lpid price pubcat_list user_attr_list score item_price item_volume item_id shop_id user_id user_xxid')
    click_item = click_item_type(*record)
    item_id = click_item.item_id
    if not click_item.item_id:
        item = db.execute("select id from item where uctrac_creative_id=%s" % click_item.creative_id)
        if item.rowcount:
            item_id = list(item)[0][0]
        else:
            item_id = 0
            logger.warn("Skipping unmatched item %s %s", click_item.click_hash, click_item.creative_id)
    sql = "insert ignore into click_item_log(outer_code, item_id, click_volume, click_price, click_time, click_ip, click_area, shop_id, user_id, user_xxid) values ('%s', %s, %s, %s, from_unixtime(%s), %s, %s, %s, %s, '%s')" % (click_item.click_hash, item_id, click_item.item_volume, click_item.item_price, click_item.click_time, click_item.click_ip, click_item.area_code, click_item.shop_id, click_item.user_id, click_item.user_xxid)
    db.execute(sql)

def clicklog_main():
    click_file_list = []
    for d in eachday(FLAGS.start, FLAGS.end):
        click_file_list.extend(glob("/space/log/filtered/click*/click-" + datestr(d) + "_00???"))
    # TODO: load from conversion db?
    ret = []
    if FLAGS.commit:
        db = get_db_engine()
    for fn in click_file_list:
        logger.debug("processing %s", fn)
        for line in open(fn, "r"):
            click = get_click(line)
            if not click:
                continue
            click_obj, click_ex_obj, score, why = click
            rec   = get_record(click)
            #if rec[0] in written:
            #    continue #already written in db.
            if rec:
                if FLAGS.commit:
                    insert_match(db, rec)
                else:
                    ret.append(rec)
    simplejson.dump(ret, open(FLAGS.out_file, "w"))
    return ret

if __name__ == "__main__":
    log_init(["GuangLogger","urlutils"], "sqlalchemy.*")
    clicklog_main()
