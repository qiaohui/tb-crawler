#!/usr/bin/env python
# coding: utf-8

#import itertools
import logging
import os
import sys
import traceback
import urllib
import time
import datetime
import re

import daemon
import gflags
import simplejson

from poster.encode import gen_boundary, MultipartParam, multipart_encode
from poster.streaminghttp import get_handlers

from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.logger import log_init
from pygaga.helpers.utils import extract_json_from_html
from pygaga.helpers.urlutils import post
from pygaga.helpers.urlutils import get_cookie_value, DEFAULT_UA
from pygaga.helpers.statsd import Statsd
from pygaga.helpers.logger import get_paperboy_logger

from qzone.utils import get_gtk

logger = logging.getLogger('QzoneLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_string('pidfile', '', "pid file location")

SHUOSHUO_URL = "http://taotao.qq.com/cgi-bin/emotion_cgi_publish_v6?g_tk=%s"
SHUOSHUO_TIMER_URL = "http://taotao.qq.com/cgi-bin/emotion_cgi_publish_timershuoshuo_v6?g_tk=%s"
UPLOAD_URL = "http://hzup.photo.qq.com/cgi-bin/upload/cgi_upload_image?boundary=%s"
UPLOAD_URL2 = "http://xaup.photo.qq.com/cgi-bin/upload/cgi_upload_image?boundary=%s"
UPLOAD_URL3 = "http://up.photo.qq.com/cgi-bin/upload/cgi_upload_image?boundary=%s"
UPLOAD_URL4 = "http://gzup.photo.qq.com/cgi-bin/upload/cgi_upload_image?boundary=%s"
DELETE_URL = "http://hz.photo.qq.com/cgi-bin/common/cgi_delpic_multi_v2?g_tk=%s"
DELETE_URL2 = "http://xa.photo.qq.com/cgi-bin/common/cgi_delpic_multi_v2?g_tk=%s"
DELETE_URL3 = "http://photo.qq.com/cgi-bin/common/cgi_delpic_multi_v2?g_tk=%s"
DELETE_URL4 = "http://gz.photo.qq.com/cgi-bin/common/cgi_delpic_multi_v2?g_tk=%s"

def log_paperboy(msg, who=['naicong',]):
    l = get_paperboy_logger()
    l.warn(msg, who)

def log_post_error(post_id, reason, table_prefix='d'):
    db = get_db_engine()
    db.execute("insert into wb_post_failed(post_id, table_prefix, reason) values (%s, '%s', '%s') on duplicate key update reason='%s', count=count+1" % (post_id, table_prefix, reason, reason))

def generate_content(qqid, content, albumid, photoid, width, height, special_url=None, schedule_ts=0):
    if type(content) is unicode:
        content = content.encode('utf-8')
    data = {
        'qzreferrer' : 'http://user.qzone.qq.com/%s' % qqid,
        'richtype' : '1',
        'private' : '0',
        'richval' : '%s,%s,%s,%s,1,%s,%s,,0,0' % (qqid, albumid, photoid, photoid, height, width),
        'who' : '1',
        'subrichtype' : '1',
        'feedversion' : '1',
        'ver' : '1',
        'code_version' : '1',
        'format' : 'fs',
        'out_charset' : 'UTF-8',
        'hostuin' : qqid,
        'con' : content
    }
    if special_url:
        data['special_url'] = special_url
    if schedule_ts:
        data['time'] = schedule_ts
    return urllib.urlencode(data)

def generate_delete(qqid, albumid, photoid):
    data = {
        'albumid' : albumid,
        'albumname' : u'私密相册贴图'.encode('gbk'),
        'appid' : 4,
        'bgid' : '',
        'codelist' : '%s|53|%s|0||%s|1|0' % (photoid, int(time.time()), photoid),
        'callbackFun' : '',
        'format' : 'fs',
        'hostUin' : qqid,
        'inCharset' : 'gbk',
        'outCharset' : 'gbk',
        'ismultiup' : 0,
        'newcover' : '',
        'notice' : 0,
        'nvip' : 0,
        'priv' : 3,
        'qzreferrer' : 'http://user.qzone.qq.com/%s' % qqid,
        'resetcover' : 0,
        'source' : 'qzone',
        'tpid' : '',
        'uin' : qqid
    }
    return urllib.urlencode(data)

def generate_multipart_photo(qqid, skey, full_filename, boundary):
    filename =  os.path.split(full_filename)[-1].split(".")[0]
    image_param = None
    try:
        image_param = MultipartParam.from_file("filename", full_filename)
    except OSError, e:
        if e.errno == 2:
            log_paperboy("File not found %s" % full_filename)
        raise e
    params = [
        ("uin", qqid),
        ("skey", skey),
        ("zzpaneluin", qqid),
        ("fileanme", filename),
        ("hd_quality", "96"),
        ("refer", "shuoshuo"),
        ("exttype", "1"),
        ("upload_hd", "1"),
        ("output_charset", "utf-8"),
        ("albumtype", "7"),
        ("filename", "filename"),
        ("uploadtype", 1),
        ("big_style", 1),
        ("charset", "utf-8"),
        ("hd_width", 2048),
        ("hd_height", 10000),
        ("output_type", "json"),
        image_param
    ]
    return multipart_encode(params, boundary=boundary)

def upload_photo2(cookiefile, full_filename, qqid, sid):
    skey = get_cookie_value(cookiefile, "skey")
    boundary = "----" + gen_boundary()
    qqid = int(qqid)
    if qqid > 2000000000:
        urls = [UPLOAD_URL % boundary, UPLOAD_URL2 % boundary, UPLOAD_URL3 % boundary, UPLOAD_URL4 % boundary]
    elif qqid > 1100000000:
        urls = [UPLOAD_URL2 % boundary, UPLOAD_URL % boundary, UPLOAD_URL3 % boundary, UPLOAD_URL4 % boundary]
    elif qqid > 1000000000:
        urls = [UPLOAD_URL3 % boundary, UPLOAD_URL2 % boundary, UPLOAD_URL % boundary, UPLOAD_URL4 % boundary]
    elif qqid < 200000000:
        urls = [UPLOAD_URL2 % boundary, UPLOAD_URL % boundary, UPLOAD_URL3 % boundary, UPLOAD_URL4 % boundary]
    else:
        urls = [UPLOAD_URL4 % boundary, UPLOAD_URL2 % boundary, UPLOAD_URL3 % boundary, UPLOAD_URL % boundary]

    photo_json = {}
    for url in urls:
        datagen, headers = generate_multipart_photo(qqid, skey, full_filename, boundary)
        headers['User-Agent'] = DEFAULT_UA
        headers['Accept'] = 'text/x-json,application/json;q=0.9,*/*;q=0.8'
        headers['Accept-Language'] = 'en-US,en;q=0.5'

        logger.info("Uploading photo %s %s -> %s", qqid, full_filename, url)
        result = post(url, datagen, headers=headers, cookiefile=cookiefile, is_accept_ending=True, ext_handlers=get_handlers())
        logger.debug("Uploaded %s %s -> %s : result %s", qqid, full_filename, url, result)
        photo_json = simplejson.loads(result.replace("_Callback(","").replace(");",""))['data']
        if photo_json.has_key('error'):
            logger.warn("Post failed qq %s -> %s %s %s", qqid, url, photo_json['error'], photo_json['msg'].encode('utf8'))
            if photo_json['error'] == -503:
                log_paperboy("Need login(photo) xks %s" % sid)
                break
        else:
            break
    return photo_json

def upload_photo(cookiefile, full_filename, qqid):
    skey = get_cookie_value(cookiefile, "skey")
    boundary = "----" + gen_boundary()
    qqid = int(qqid)
    if qqid > 2000000000:
        url = UPLOAD_URL % boundary
    elif qqid > 1100000000:
        url = UPLOAD_URL2 % boundary
    elif qqid > 1000000000:
        url = UPLOAD_URL3 % boundary
    elif qqid < 200000000:
        url = UPLOAD_URL2 % boundary
    else:
        url = UPLOAD_URL4 % boundary

    datagen, headers = generate_multipart_photo(qqid, skey, full_filename, boundary)
    headers['User-Agent'] = DEFAULT_UA
    headers['Accept'] = 'text/x-json,application/json;q=0.9,*/*;q=0.8'
    headers['Accept-Language'] = 'en-US,en;q=0.5'

    logger.info("Uploading photo %s %s -> %s", qqid, full_filename, url)
    result = post(url, datagen, headers=headers, cookiefile=cookiefile, is_accept_ending=True, ext_handlers=get_handlers())
    logger.debug("Uploaded %s %s -> %s : result %s", qqid, full_filename, url, result)
    return result

def delete_photo(cookiefile, qqid, photo_json, sid):
    skey = get_cookie_value(cookiefile, "skey")
    gtk = get_gtk(skey)
    qqid = int(qqid)
    if qqid > 2000000000:
        url = DELETE_URL % gtk
    elif qqid > 1100000000:
        url = DELETE_URL2 % gtk
    elif qqid > 1000000000:
        url = DELETE_URL3 % gtk
    elif qqid < 200000000:
        url = DELETE_URL2 % gtk
    else:
        url = DELETE_URL4 % gtk

    data = generate_delete(qqid, photo_json['albumid'], photo_json['lloc'])
    logger.info("Deleting failed photo %s %s", qqid, url)
    result = post(url, data, cookiefile=cookiefile).decode('gbk').encode('utf8')
    logger.debug("Deleting %s result: %s", qqid, result)
    delete_json = extract_json_from_html(result, 'frameElement.callback')
    if (delete_json['code'] < 0):
        logger.warn("Deleting %s failed %s code %s, %s", qqid, delete_json['message'].encode('utf8'), delete_json['code'], delete_json['subcode'])
    if delete_json['code'] == -3000:
        log_paperboy('Need login(del) xks %s' % sid)
    return result

def post_content(cookiefile, qqid, content, albumid, photoid, photo_width, photo_height, special_url=None, schedule_ts=0):
    skey = get_cookie_value(cookiefile, "skey")
    gtk = get_gtk(skey)
    if schedule_ts:
        url = SHUOSHUO_TIMER_URL % gtk
    else:
        url = SHUOSHUO_URL % gtk

    data = generate_content(qqid, content, albumid, photoid, photo_width, photo_height, special_url, schedule_ts)

    logger.info("Posting content %s with photo", qqid)
    result = post(url, data, cookiefile=cookiefile)
    logger.debug("Posting %s result: %s, timer %s", qqid, result, schedule_ts)
    return result

def post_shuoshuo(cookiefile, photofile, content, sid=0, schedule_ts=0, post_id=0):
    if type(cookiefile) is unicode:
        cookiefile = cookiefile.encode('utf8')
    if type(photofile) is unicode:
        photofile = photofile.encode('utf8')
    if type(content) is unicode:
        content = content.encode('utf8')
    qqid = 0
    result = False
    try:
        qqid = get_cookie_value(cookiefile, "ptui_loginuin")
        photo_json = upload_photo2(cookiefile, photofile, qqid, sid)

        if photo_json.has_key('error'):
            logger.error("Post failed qq %s, xks %s, %s, %s, %s --> %s reason %s", qqid, sid, cookiefile, photofile, content, photo_json['error'], photo_json['msg'].encode('utf8'))
            log_post_error(post_id, "%s : %s" % (photo_json['error'], photo_json['msg'].encode('utf8')))
            return False
        albumid = photo_json['albumid']
        photoid = photo_json['lloc']
        width = photo_json['width']
        height = photo_json['height']
        special_url = photo_json['micro']['url']
        #TODO: save into wb_photojson if failed

        post_result = post_content(cookiefile, qqid, content, albumid, photoid, width, height, special_url, schedule_ts)
        #import pdb; pdb.set_trace()
        if (not schedule_ts and post_result.find("content_box") > 0):
            result = True
        elif schedule_ts:
            content_json = extract_json_from_html(post_result, 'frameElement.callback')
            if content_json['code'] < 0:
                logger.warn("Post failed qq %s, xks %s, %s, %s, %s, %s, %s, %s", qqid, sid, cookiefile, photofile, content, content_json['message'].encode('utf8'), content_json['code'], content_json['subcode'])
                log_post_error(post_id, "%s,%s : %s" % (content_json['code'], content_json['subcode'], content_json['message'].encode('utf8')))
                if content_json['code'] == -3000:
                    log_paperboy('Need login(content) xks %s' % sid)
                delete_photo(cookiefile, qqid, photo_json, sid)
            elif content_json.has_key('richinfo'):
                result = True
        else:
            logger.warn("Post failed qq %s, xks %s, %s, %s, %s", qqid, sid, cookiefile, photofile, content)
            delete_photo(cookiefile, qqid, photo_json, sid)
            log_post_error(post_id, "-2 : %s" % post_result)
        if (not schedule_ts and post_result.find("content_box") > 0) or (schedule_ts and post_result.find("richinfo") > 0):
            result = True
    except KeyboardInterrupt:
        raise
    except:
        logger.error("Post failed qq %s, xks %s, %s, %s, %s --> reason %s", qqid, sid, cookiefile, photofile, content, traceback.format_exc())
        log_post_error(post_id, "-1 : exception" )
    return result

def post_shuoshuo_string(cookie, photofile, content, sid=0, schedule_ts=0, post_id=0):
    cookiefile = os.tempnam('/tmp', 'shuoshuo_cookie_')
    f = open(cookiefile, "w")
    f.write(cookie)
    f.close()
    if not FLAGS.dryrun:
        result = post_shuoshuo(cookiefile, photofile, content, sid=sid, schedule_ts=schedule_ts, post_id=post_id)
    os.remove(cookiefile)
    return result

def post_content_wget(cookies_path, data):
    skey = get_cookie_value(cookies_path, "skey")
    gtk = get_gtk(skey)
    cmd = """wget -S -U "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)" --load-cookies %s --post-data "%s" "http://taotao.qq.com/cgi-bin/emotion_cgi_publish_v6?g_tk=%s"  """ % (cookies_path, data, gtk)
    return os.system(cmd)

def preprocess_content(content, sid, post_ts, table_prefix, post_id):
    def change_href(m):
        src = "src=qzone%s_%s_%s_%s" % (time.strftime('%y%m%d%H%M', post_ts.timetuple()), sid, table_prefix, post_id)
        if m.group(1).find("?") < 0:
            href = m.group(1) + "?" + src
        else:
            href = m.group(1) + "&" + src
        if href.find("xks=") < 0:
            href += "&xks=" + str(sid)
        return href
    return re.sub("(http\S+)", change_href, content)

def post_one(db, user, select_sql, update_succ_sql, update_fail_sql, table_prefix):
    now = datetime.datetime.now()
    next = datetime.datetime(2020, 1, 1)
    total = 0
    succ = 0
    failed = 0
    skip = 0

    logger.debug("querying %s", select_sql)
    results = db.execute(select_sql)
    logger.debug("processing post for user %s, total %s, sql %s", user, results.rowcount, select_sql)
    for item in results:
        if FLAGS.postinterval:
            time.sleep(FLAGS.postinterval)
        total += 1
        logger.debug("processing post %s/%s/%s/%s for user %s", succ, skip, total, results.rowcount, user)
        post_id = item[0]
        filename = item[2]
        post_ts = item[3]
        qqid = item[4]
        cookie_file = item[5]
        sid = item[6]
        #grplevel = item[7]
        content = preprocess_content(item[1], sid, post_ts, table_prefix, post_id)
        # reselect cookie
        cookie_result = list(db.execute("select cookies from wb_qq_account where qqid=%s" % qqid))
        if cookie_result and cookie_result[0][0] != cookie_file:
            cookie_file = cookie_result[0][0]
        if post_ts <= now and cookie_file:
            logger.info("Preparing posting %s/%s %s qq %s sid %s %s %s @ %s", total, results.rowcount, post_id, qqid, sid, content.encode('utf8'), filename.encode('utf8'), post_ts)
            result = post_shuoshuo_string(cookie_file, filename, content, sid=sid, post_id=post_id)
            if not FLAGS.dryrun:
                if result:
                    succ += 1
                    db.execute(update_succ_sql % post_id)
                    Statsd.increment('guang.qzonepost.succ')
                elif FLAGS.commitfail:
                    failed += 1
                    db.execute(update_fail_sql % post_id)
                    Statsd.increment('guang.qzonepost.fail')
                    log_paperboy("post timeout xks %s post_id %s" % (sid, post_id))
        else:
            if FLAGS.timer:
                logger.info("Preparing posting timer %s/%s %s qq %s sid %s %s %s @ %s", total, results.rowcount, post_id, qqid, sid, content.encode('utf8'), filename.encode('utf8'), post_ts)
                result = post_shuoshuo_string(cookie_file, filename, content, sid=sid, schedule_ts=int(time.mktime(post_ts.timetuple())), post_id=post_id)
                if not FLAGS.dryrun:
                    if result:
                        succ += 1
                        db.execute(update_succ_sql % post_id)
                        Statsd.increment('guang.qzonepost.succ')
                    else:
                        skip += 1
                        next = min(post_ts, next)
                        Statsd.increment('guang.qzonepost.timerfail')
            else:
                skip += 1
                next = min(post_ts, next)
                logger.debug("Skiping post %s %s, scheduled @ %s", content.encode('utf8'), filename.encode('utf8'), post_ts)
    if total > 0:
        logger.info("Batch result total %s skip %s succ %s failed %s next schedule %s", total, skip, succ, failed, next)

def post_from_db_one(user=None):
    db = get_db_engine()
    if user >= 0:
        select_sql = "SELECT wbd.id,wbd.content,CONCAT('/space/antbuild.guangadmin/guang-admin_pics/produce/',wbd.pic_name) pic_path,wbd.send_time,qqid,cookies,wb_xks_info.sid,wbd.grplevel FROM wb_product_content_detail wbd,wb_product_content wb,wb_xks_info,wb_qq_account WHERE wb_xks_info.sid=wbd.sid and wb_xks_info.qq_num=wb_qq_account.qqid and wb.id=wbd.wbpc_id AND wb.status=2 AND wbd.status=0 AND wb_xks_info.sid=%s AND (wb.platforms&3 != 0) ORDER BY wbd.send_time;" % user
    else:
        select_sql = "SELECT wbd.id,wbd.content,CONCAT('/space/antbuild.guangadmin/guang-admin_pics/produce/',wbd.pic_name) pic_path,wbd.send_time,qqid,cookies,wb_xks_info.sid,wbd.grplevel FROM wb_product_content_detail wbd,wb_product_content wb,wb_xks_info,wb_qq_account WHERE wb_xks_info.sid=wbd.sid and wb_xks_info.qq_num=wb_qq_account.qqid and wb.id=wbd.wbpc_id AND wb.status=2 AND wbd.status=0 AND (wb.platforms&3 != 0) ORDER BY wbd.send_time;"
    update_succ_sql = "update wb_product_content_detail set status=3 where id=%s"
    update_fail_sql = "update wb_product_content_detail set status=-3 where id=%s"
    post_one(db, user, select_sql, update_succ_sql, update_fail_sql, "d")

    if user >= 0:
        return
    # old way
    select_sql2 = "SELECT wc.id,REPLACE(wc.content,'#replace#','') content,CONCAT('/space/antbuild.guangadmin/ROOT/resources/img/upload/',wc.pic_name) pic_path,wc.send_time,qqid,cookies,wb_xks_info.sid,1 FROM wb_content wc,wb_content_group wcg,wb_group_account wga,wb_account wa,wb_xks_info,wb_qq_account WHERE wc.id=wcg.content_id AND wcg.group_id=wga.group_id AND wga.account_id=wa.id AND wb_xks_info.qq_num=wb_qq_account.qqid AND wb_xks_info.wbname=wa.name AND wc.status=0 ORDER BY send_time;"
    update_succ_sql2 = "update wb_content set status=4 where id=%s"
    update_fail_sql2 = "update wb_content set status=-3 where id=%s"
    post_one(db, user, select_sql2, update_succ_sql2, update_fail_sql2, "c")

def post_from_db(user=None):
    while True:
        try:
            post_from_db_one(user)
            if FLAGS.loop:
                time.sleep(FLAGS.interval)
            else:
                break
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    gflags.DEFINE_string('cookie', "/Users/chris/tmp/qqtest/cookies.txt", "cookie path", short_name="k")
    gflags.DEFINE_string('photo', "/Users/chris/tmp/1.jpg", "photo path", short_name="p")
    gflags.DEFINE_string('content', "", "post content", short_name="c")
    gflags.DEFINE_boolean('daemon', False, "run as daemon")
    gflags.DEFINE_boolean('fromdb', True, "post content from db")
    gflags.DEFINE_boolean('dryrun', False, "dry run, not post and update db")
    gflags.DEFINE_boolean('commitfail', True, "is commit status to database when failed")
    gflags.DEFINE_boolean('loop', False, "is loop forever?")
    gflags.DEFINE_boolean('timer', False, "is use timer post?")
    gflags.DEFINE_boolean('test', False, "is test mode? not post, just check")
    gflags.DEFINE_integer('sid', -1, "post one user from db")
    gflags.DEFINE_integer('interval', 20, "sleep seconds between post")
    gflags.DEFINE_integer('postinterval', 0, "sleep seconds between post")
    log_init('QzoneLogger', "sqlalchemy.*")
    #log_init('QzoneLogger', "")
    if FLAGS.daemon:
        if not FLAGS.pidfile:
            pidfile = os.path.join(os.path.split(os.path.abspath(__file__))[0], 'post.pid')
        else:
            pidfile = FLAGS.pidfile
        daemon.daemonize(pidfile)
    if not FLAGS.fromdb:
        result = post_shuoshuo(FLAGS.cookie, FLAGS.photo, FLAGS.content)
        if result:
            logger.info("Uploading content success")
            sys.exit(0)
        else:
            sys.exit(1)
    else:
        if FLAGS.sid:
            post_from_db(FLAGS.sid)
        else:
            post_from_db()
