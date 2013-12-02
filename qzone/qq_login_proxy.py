#!/usr/bin/env python
# coding: utf-8

import datetime
import os
import urllib2
import sys
import time

import web
import daemon
import gflags
import logging

from jinja2 import Environment
from jinja2 import FileSystemLoader

from pygaga.helpers.urlutils import get_cookie_opener, DEFAULT_UA
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('QzoneLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_string('pidfile', '', "pid file location")

file_path = os.path.split(os.path.abspath(__file__))[0]
template_path = os.path.join(file_path, 'templates')
if not os.path.exists(template_path):
    template_path = os.path.join(file_path, '../../qzone/templates')
ENV = Environment(loader = FileSystemLoader(template_path, encoding='utf8'), auto_reload=True)

urls = (
    '/qq2012/.*', 'qqloginsuccess',
    '/cgi-bin/login', 'qqlogin',
    '/cookies.txt', 'cookies',
    '/', 'home',
    '/xks/(.*)', 'xks',
)

def render_to_string(template_name, context):
    template = ENV.get_template(template_name)
    return template.render(context)

def render_html(template_name, context):
    web.header("Content-Type", "text/html; charset=utf-8")
    return render_to_string(template_name, context)

def generate_cookiefile(cookies):
    t = time.time() + 3600
    uin = " "
    lines = []
    for c in cookies.items():
        #if c[0] in ('skey', 'RK', 'ptui_loginuin', 'pt2gguin', 'pgv_info', 'pgv_pvid', 'verifysession', 'ptisp', 'o_cookie'):
        if c[0] in ('skey', 'RK', 'ptui_loginuin', 'pt2gguin', 'pgv_info', 'pgv_pvid', 'verifysession', 'ptisp'):
            if c[0] == "pt2gguin":
                uin = c[1]
            lines.append(".qq.com\tTRUE\t/\tFALSE\t"+str(int(t))+"\t"+c[0]+"\t"+(c[1] if c[1] else " ")+"\n")
    lines.append(".qq.com\tTRUE\t/\tFALSE\t"+str(int(t))+"\tuin\t"+uin+"\n")
    return "".join(lines)

class qqlogin:
    QQ_REFER = "http://www.qq.com/"
    headers = {'User-Agent' : DEFAULT_UA, 'Referer' : QQ_REFER}

    def GET(self):
        self.QQLOGIN_URL = "http://ui.ptlogin2.qq.com/cgi-bin/login?hide_title_bar=0&low_login=0&qlogin_auto_login=1&no_verifyimg=1&link_target=blank&appid=636014201&target=self&s_url=http%3A//" + FLAGS.qqhost + ":" + str(FLAGS.qqport) + "/qq2012/loginSuccess.htm"

        args = web.input(id='', passwd='langtaojin')
        opener = get_cookie_opener(is_accept_ending=False)
        req = urllib2.Request(self.QQLOGIN_URL, headers=self.headers)
        u = opener.open(req)
        result = u.read()
        pos = result.find('</html>')
        result = result[:pos] + '<script>document.getElementById("u").value="' + args['id'].encode('utf-8') +             '";document.getElementById("p").value="' + args['passwd'].encode('utf-8') + '";</script></html>'
        web.header("Content-Type", "text/html; charset=utf-8")
        return result

class qqloginsuccess:
    def GET(self):
        #args = web.input()
        cookies = web.cookies()
        web.header("Content-Type", "text/html; charset=utf-8")
        cookie_content = generate_cookiefile(cookies)

        qqid = cookies['ptui_loginuin']
        db = web.database(dbn='mysql', db='guangbi', user='guangbi', pw='guangbi', port=FLAGS.dbport, host=FLAGS.dbhost)
        logger.info("updating database")
        db.update('wb_qq_account', where='qqid=%s' % qqid, last_login=datetime.datetime.now(), cookies=cookie_content)
        logger.info("updated database")

        open(FLAGS.dumpcookiepath, 'w').write(cookie_content)
        # remove any cookies
        web.setcookie('pgv_pvid', '', -1)
        web.setcookie('o_cookie', '', -1)
        return generate_cookiefile(cookies).replace("\n", "<br>")

class cookies:
    def GET(self):
        args = web.input(id='')
        db = web.database(dbn='mysql', db='guangbi', user='guangbi', pw='guangbi', port=FLAGS.dbport, host=FLAGS.dbhost)
        result = db.select('wb_qq_account', where='qqid=%s' % args['id'])
        web.header("Content-Type", "text/plain; charset=utf-8")
        return result[0]['cookies']

class xks:
    def GET(self, id):
        db = web.database(dbn='mysql', db='guangbi', user='guangbi', pw='guangbi', port=FLAGS.dbport, host=FLAGS.dbhost)
        #items = db.select("wb_product_content_detail,wb_product_content", what="wb_product_content_detail.id,wb_product_content_detail.content,wb_product_content_detail.send_time,wb_product_content_detail.grplevel,wb_product_content_detail.pic_name", where="wb_product_content.id=wb_product_content_detail.wbpc_id AND wb_product_content.status=2 AND wb_product_content_detail.status=0 AND sid=%s" % id)
        items = db.query("select wb_product_content_detail.id,wb_product_content_detail.content,wb_product_content_detail.send_time,wb_product_content_detail.grplevel,wb_product_content_detail.pic_name,wb_post_failed.reason,wb_post_failed.last_time,wb_post_failed.count from wb_product_content,wb_product_content_detail left join wb_post_failed on wb_post_failed.post_id=wb_product_content_detail.id where wb_product_content.id=wb_product_content_detail.wbpc_id AND wb_product_content.status=2 AND wb_product_content_detail.status=0 AND (wb_product_content.platforms&3 != 0) AND sid=%s;" % id)
        return render_html("xks.htm", {"items" : items})

class home:
    def GET(self):
        if web.ctx.env.get('HTTP_HOST', '').find('qq.com') < 0:
            return "<html><body>Hi, wolrd >_< </body></html>"
        db = web.database(dbn='mysql', db='guangbi', user='guangbi', pw='guangbi', port=FLAGS.dbport, host=FLAGS.dbhost)
        result = db.select("wb_qq_account,wb_xks_info", what="sname,wbname,qqid,last_login,cookies,passwd,sid", where="wb_qq_account.qqid=wb_xks_info.qq_num")
        remain_items = dict([(r['sid'],r['count']) for r in db.select("wb_product_content_detail,wb_product_content", what="sid,count(wb_product_content_detail.id) as count", where="wb_product_content.id=wb_product_content_detail.wbpc_id AND wb_product_content.status=2 AND wb_product_content_detail.status=0 AND (wb_product_content.platforms&3 != 0)", group="sid")])
        return render_html("home.htm", {'accounts' : result,
            'remains' : remain_items,
            'now' : datetime.datetime.now(),
            'delta' : datetime.timedelta(0, 3600)})

if __name__ == "__main__":
    gflags.DEFINE_boolean('daemon', False, "is start in daemon mode?")
    gflags.DEFINE_boolean('webdebug', False, "is web.py debug")
    gflags.DEFINE_boolean('reload', False, "is web.py reload app")
    gflags.DEFINE_string('qqhost', 'test.qq.com', "fake qq host")
    gflags.DEFINE_string('dumpcookiepath', '/tmp/qq_cookie.txt', "dump cookie path")
    gflags.DEFINE_integer('qqport', 8025, "fake qq port")
    backup_args = []
    backup_args.extend(sys.argv)
    sys.argv = [sys.argv[0],] + sys.argv[2:]
    log_init('QzoneLogger', "sqlalchemy.*")
    sys.argv = backup_args[:2]
    web.config.debug = FLAGS.webdebug
    if len(sys.argv) == 1:
        web.wsgi.runwsgi = lambda func, addr=None: web.wsgi.runfcgi(func, addr)
    else:
        FLAGS.qqport = sys.argv[1]
    if FLAGS.daemon:
        if not FLAGS.pidfile:
            pidfile = os.path.join(file_path, 'qq_login_proxy.pid')
        else:
            pidfile = FLAGS.pidfile
        daemon.daemonize(pidfile)
    #render = web.template.render('templates/', base='layout')
    app = web.application(urls, globals(), autoreload=FLAGS.reload)
    app.run()
