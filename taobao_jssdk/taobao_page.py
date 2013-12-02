
import datetime
import re
import urllib2
import sys
import time
import hmac
import hashlib

import web
import gflags
import logging

from jinja2 import Environment
from jinja2 import FileSystemLoader

from pygaga.helpers.urlutils import get_cookie_opener, DEFAULT_UA, download
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('XtaoLogger')

FLAGS = gflags.FLAGS

ENV = Environment(loader = FileSystemLoader('templates', encoding='utf8'), auto_reload=True)

urls = (
    '/', 'home',
)

APPKEY=12669715
APPSECRET="7062800942c7b6f18f6a393a364d812f"

def render_to_string(template_name, context):
    template = ENV.get_template(template_name)
    return template.render(context)

def render_html(template_name, context):
    web.header("Content-Type", "text/html; charset=utf-8")
    return render_to_string(template_name, context)

#render = web.template.render('templates/', base='layout')
app = web.application(urls, globals(), autoreload=True)

class home:
    def GET(self):
        db = web.database(dbn='mysql', db='guang', user='guang', pw='guang', port=FLAGS.dbport, host=FLAGS.dbhost)
        result = db.select("item", what="id,num_id,detail_url,pic_url", where="status=1 and detail_url not like '%s.click.taobao.com%'", order="id desc", limit=40)
        ts = int(time.time()*1000)
        #import pdb; pdb.set_trace()
        msg = APPSECRET + 'app_key' + str(APPKEY) + "timestamp" + str(ts) + APPSECRET
        sign = hmac.HMAC(APPSECRET, msg).hexdigest().upper()
        web.setcookie('timestamp', str(ts))
        web.setcookie('sign', sign)
        return render_html("home.htm", {'items' : result,
            })

if __name__ == "__main__":
    gflags.DEFINE_boolean('webdebug', False, "is web.py debug")
    gflags.DEFINE_integer('xtaoport', 8025, "fake qq port")
    backup_args = []
    backup_args.extend(sys.argv)
    sys.argv = [sys.argv[0],] + sys.argv[2:]
    log_init('XtaoLogger', "sqlalchemy.*")
    sys.argv = backup_args[:2]
    web.config.debug = FLAGS.webdebug
    if len(sys.argv) == 1:
        web.wsgi.runwsgi = lambda func, addr=None: web.wsgi.runfcgi(func, addr)
    else:
        FLAGS.xtaoport = sys.argv[1]
    app.run()

