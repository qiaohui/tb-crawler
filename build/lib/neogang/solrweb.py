#!/usr/bin/env python
# coding: utf-8

#import datetime
#import re
import os
import urllib
#import urllib2
import sys
#import time

import web
import daemon
import gflags
import logging
import simplejson

#from cStringIO import StringIO

from jinja2 import Environment
from jinja2 import FileSystemLoader

#from pygaga.helpers.urlutils import get_cookie_opener, DEFAULT_UA, download
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download
#from pygaga.helpers.colorutils import colorz, colors_as_image
#from colorific import extract_colors

logger = logging.getLogger('GuangLogger')

FLAGS = gflags.FLAGS

file_path = os.path.split(os.path.abspath(__file__))[0]

ENV = Environment(loader = FileSystemLoader(os.path.join(file_path, 'templates'), encoding='utf8'), auto_reload=True)

urls = (
    '/', 'list_page',
    '/cust', 'custom_list_page',
    '/cust2', 'custom_list_page2',
    #'/p.png', 'palette_png',
    #'/img_extract.png', 'extract_png',
)

def render_to_string(template_name, context):
    template = ENV.get_template(template_name)
    return template.render(context)

def render_html(template_name, context):
    web.header("Content-Type", "text/html; charset=utf-8")
    return render_to_string(template_name, context)

def lazyextract_filter(doc):
    src = "http://image2.guang.j.cn/images/%s/mid2/%s" % (doc['shop_id'], doc['item_local_picurl'])
    return "<img class='lazy' data-original='/img_extract.png?%s' width=300 height=300>" % urllib.urlencode({'src':src})

"""
def color_filter(doc):
    img_src = "http://image2.guang.j.cn/images/%s/mid2/%s" % (doc['shop_id'], doc['item_local_picurl'])
    img_data = download(img_src)
    if img_data:
        palette = extract_colors(StringIO(img_data), is_auto_crop=False, is_auto_detect=False, is_manual_crop=True, manual_crop_percent=(0.2,0.2,0.8,0.8))
        color_str = ','.join(['%2x%2x%2x' % p.value for p in palette.colors])
        if not palette.bgcolor:
            return "<img class='lazy' original='/p.png?c=%s'>" % color_str
        else:
            bgcolor_str = '%2x%2x%2x' % palette.bgcolor.value
            return "<img class='lazy' original='/p.png?c=%s'> BG <img src='/p.png?c=%s'>" % (color_str, bgcolor_str)
    return ""

def color_filter2(doc):
    img_src = "http://image2.guang.j.cn/images/%s/mid2/%s" % (doc['shop_id'], doc['item_local_picurl'])
    img_data = download(img_src)
    if img_data:
        colors = colorz(StringIO(img_data), 5, manual_crop_percent=(0.2,0.2,0.8,0.8))
        color_str = ','.join([p[1:] for p in colors])
        return "<img src='/p.png?c=%s'>" % color_str
    return ""
"""

def render_timing(timing):
    def render_sub_timing(name, subtiming):
        sub_result = "</div><div class='row'><div class='offset1 span6'>%s timing : %s</div></div>" % (name, subtiming['time'])
        for t in subtiming:
            if t != 'time' and subtiming[t]['time'] - 0.0 > 0.000001:
                sub_result += "<div class='row'><div class='offset2 span6'>%s : %s</div></div>" % (t, subtiming[t]['time'])
        return sub_result
    result = "<div class='row'><div class='span6'>total timing : %s</div></div>" % timing['time']
    for sub in timing:
        if sub != 'time' and timing[sub]['time'] - 0.0 > 0.000001:
            result += render_sub_timing(sub, timing[sub])
    return result

def get_boolean(v):
    if v in ('on', 'true', 'True', 'TRUE'):
       return "true"
    return ""

def get_string(v):
    if v:
        return v
    return ""

def get_full_cust_url(params):
    debugQuery = get_boolean(params.debugQuery)
    return "/cust?term_id=%s&start=%s&rows=%s&sortby=%s&xks=%s&debugQuery=%s" % (
        params.term_id, params.start, params.rows, params.sortby, get_string(params.xks), get_string(debugQuery)
    )

def get_full_url(params):
    edismax = get_boolean(params.edismax)
    debugQuery = get_boolean(params.debugQuery)
    return "/?term_id=%s&start=%s&rows=%s&sortby=%s&xks=%s&debugQuery=%s&edismax=%s&qf=%s&boost=%s&bfs=%s" % (
        params.term_id, params.start, params.rows, params.sortby, get_string(params.xks), get_string(debugQuery),
        edismax, get_string(params.qf), get_string(params.boost), get_string(params.bfs)
    )

def convert_br(s):
    return s.replace("\n", "<br>").replace(' ', '&nbsp;')

ENV.filters['lazy_extract_color'] = lazyextract_filter
#ENV.filters['extract_color'] = color_filter
#ENV.filters['extract_color2'] = color_filter2
ENV.filters['render_timing'] = render_timing
ENV.filters['get_full_url'] = get_full_url
ENV.filters['get_full_cust_url'] = get_full_cust_url
ENV.filters['convert_br'] = convert_br

def build_solr_custom_qs2(term_id, start, rows, sortby, debugQuery, wd, tagmatch):
    params = [
        ('sort', '%s desc' % sortby),
        ('rows', rows),
        ('wt', 'json'),
        ('fl', '*,score'),
        ('start', start),
        ('version', '2'),
        ('fq', 'term_id:%s' % term_id),
        ('uid', '1426277'),
        ('q', '*:*'),
        ('fq', 'item_id:[0 TO *]'),
        ('debug', 'true'),
        ]
    if tagmatch:
        if tagmatch.endswith(";"):
            tagmatch = tagmatch[:-1]
        params.append(('tagmatch', tagmatch))
    if debugQuery:
        params.append(('debugQuery', debugQuery))
    #url = "http://%s:7080/solr/customselect?q=*:*&fq=item_id:[0+TO+*]&%s" % (FLAGS.solr_host, urllib.urlencode(params))
    url = "http://%s:7080/solr/customselect?%s" % (FLAGS.solr_host, urllib.urlencode(params))
    return url

def build_solr_custom_qs(term_id, start, rows, sortby, debugQuery, wd, tagmatch):
    params = [
        ('rows', rows),
        ('wt', 'json'),
        ('fl', '*,score'),
        ('start', start),
        ('version', '2'),
        ('fq', 'item_id:[0 TO *]'),
        ('fq', 'term_id:%s' % term_id),
        ('debug', 'true'),
        ]
    if wd:
        wd = wd.encode('utf8')
        params.append(('q', '(%s) AND _val_:"customfunc(%s,0,00033a55d7cbe5211b0458b1ea8eede1,%s)"' % (' AND '.join([w.strip() for w in wd.split(' ') if w.strip()]), sortby, tagmatch.replace(";", ","))))
    else:
        params.append(('q', '(*:*) AND _val_:"customfunc(%s,0,00033a55d7cbe5211b0458b1ea8eede1,%s)"' % (sortby, tagmatch.replace(";", ","))))
    if tagmatch:
        params.append(('tagmatch', tagmatch))
    if debugQuery:
        params.append(('debugQuery', debugQuery))
    url = "http://%s:7080/solr/customselect?%s" % (FLAGS.solr_host, urllib.urlencode(params))
    return url

def build_solr_qs(term_id, start, rows, sortby, edismax, qf, bfs, boost, debugQuery, wd):
    params = [
        ('rows', rows),
        ('wt', 'json'),
        ('fl', '*,score'),
        ('start', start),
        ('version', '2'),
        ('fq', 'item_id:[0 TO *]'),
        ('fq', 'term_id:%s' % term_id),
        ]
    if wd:
        wd = wd.encode('utf8')
        params.append(('q', '%s' % (' AND '.join([w.strip() for w in wd.split(' ') if w.strip()]))))
    else:
        params.append(('q', '*:*'))
    if debugQuery:
        params.append(('debugQuery', debugQuery))
    if edismax:
        params.append(('defType', 'edismax'))
        if qf:
            params.append(('qf', 'item_title^1.4 shop_name^0.5'))
        for bf in bfs:
            params.append(('bf', bf))
        if boost:
            params.append(('boost', boost))
    else:
        params.append(('sort', '%s desc' % sortby))

    url = "http://%s:7080/solr/select?%s" % (FLAGS.solr_host, urllib.urlencode(params))
    return url

def convert_tagmatch(tm):
    if tm == 0:
        return "1:0.2;2:0.2;3:0.2;4:0.2;5:0.2;6:0.2;"
    elif tm < 100:
        return '%s:1.0;' % (tm/10)
    elif tm < 10000:
        tm2 = str(tm)
        if tm2[1] == tm2[3]:
            return "%s:0.5;%s:0.5;" % (tm2[0], tm2[2])
        elif tm2[1] == '1':
            return "%s:0.7;%s:0.3;" % (tm2[0], tm2[2])
        else:
            return "%s:0.3;%s:0.7;" % (tm2[0], tm2[2])
    else:
        return ""

def get_xks_tagmatch(xks):
    tagmatch = ''
    if xks:
        db = get_db_engine()
        rows = db.execute("SELECT tag_match FROM recommend_subscriber WHERE id = %s" % xks)
        if rows.rowcount > 0:
            tagmatch = convert_tagmatch(list(rows)[0][0])
    return tagmatch

def replace_meta(q, sortby, tagmatch):
    result = q.replace('__SORT__', sortby)
    return result.replace('__SIMI__', 'tagdist(guang_tag_match,%s)' % tagmatch.replace(";", ","))

class custom_list_page2:
    def GET(self):
        params = web.input(term_id=74, start=0, rows=120,
            sortby='region_ctr_0111_4',
            xks=12,
            wd='',
            debugQuery='on')
        tagmatch = get_xks_tagmatch(params.xks)
        url = build_solr_custom_qs2(params.term_id, params.start, params.rows, params.sortby,
            params.debugQuery, params.wd, tagmatch)
        logger.debug('fetching %s', url)
        results = simplejson.loads(download(url))
        #import pdb; pdb.set_trace()
        return render_html("custlist.htm", {'results' : results,
                'solrurl' : url,
                'xksinfo' : 'xks %s : tagmatch %s' % (params.xks, tagmatch),
                'params' : params,
                })

class custom_list_page:
    def GET(self):
        params = web.input(term_id=74, start=0, rows=120,
            sortby='region_ctr_0111_4',
            xks=12,
            wd='',
            debugQuery='on')
        tagmatch = get_xks_tagmatch(params.xks)
        url = build_solr_custom_qs(params.term_id, params.start, params.rows, params.sortby,
            params.debugQuery, params.wd, tagmatch)
        logger.debug('fetching %s', url)
        results = simplejson.loads(download(url))
        #import pdb; pdb.set_trace()
        return render_html("custlist.htm", {'results' : results,
                'solrurl' : url,
                'xksinfo' : 'xks %s : tagmatch %s' % (params.xks, tagmatch),
                'params' : params,
                })

class list_page:
    def GET(self):
        params = web.input(term_id=74, start=0, rows=120,
            sortby='region_ctr_0111_4',
            edismax=False,
            bfs='sum(mul(__SORT__,2.5),mul(__SIMI__,5.0))',
            qf='item_title^1.4 shop_name^0.5',
            boost=None,
            xks=12,
            wd='',
            debugQuery='on')
        bfs = boost = ''
        tagmatch = get_xks_tagmatch(params.xks)
        if params.edismax and params.bfs:
            bfs = replace_meta(params.bfs, params.sortby, tagmatch)
        if params.edismax and params.boost:
            boost = replace_meta(params.boost, params.sortby, tagmatch)
        url = build_solr_qs(params.term_id, params.start, params.rows, params.sortby,
            params.edismax, params.qf, bfs.split('|'), boost, params.debugQuery, params.wd)
        logger.debug('fetching %s', url)
        results = simplejson.loads(download(url))
        #import pdb; pdb.set_trace()
        return render_html("list.htm", {'results' : results,
                'solrurl' : url,
                'xksinfo' : 'xks %s : tagmatch %s' % (params.xks, tagmatch),
                'params' : params,
                })

"""
class extract_png:
    def GET(self):
        web.header("Content-Type", "images/png")
        params = web.input()
        img_src = params.src;
        img_data = download(img_src)
        if img_data:
            palette = extract_colors(StringIO(img_data), is_auto_crop=False, is_auto_detect=False, is_manual_crop=True, manual_crop_percent=(0.2,0.2,0.8,0.8))
            data = StringIO()
            colors_as_image(['%2x%2x%2x' % c.value for c in palette.colors]).save(data, 'png')
            data.seek(0)
            return data.read()
        return ""

class palette_png:
    def GET(self):
        web.header("Content-Type", "images/png")
        params = web.input()
        data = StringIO()
        colors_as_image(params.c.split(",")).save(data, 'png')
        data.seek(0)
        return data.read()
"""

if __name__ == "__main__":
    gflags.DEFINE_boolean('daemon', False, "is start in daemon mode?")
    gflags.DEFINE_boolean('webdebug', False, "is web.py debug")
    gflags.DEFINE_boolean('reload', False, "is web.py reload app")
    gflags.DEFINE_string('solr_host', 'sdl-guang-solr4', 'solr host')
    backup_args = []
    backup_args.extend(sys.argv)
    sys.argv = [sys.argv[0],] + sys.argv[2:]
    log_init('GuangLogger', "sqlalchemy.*")
    sys.argv = backup_args[:2]
    web.config.debug = FLAGS.webdebug
    if len(sys.argv) == 1:
        web.wsgi.runwsgi = lambda func, addr=None: web.wsgi.runfcgi(func, addr)
    if FLAGS.daemon:
        daemon.daemonize(os.path.join(file_path, 'solrweb.pid'))
    #render = web.template.render('templates/', base='layout')
    app = web.application(urls, globals(), autoreload=FLAGS.reload)
    app.run()
