#!/usr/bin/env python
# coding: utf-8

import cStringIO
import Image
import urllib2
import gflags
import re
import os
import sys
import web
import traceback

from guang_crawler.image_crawler import ItemCrawler
from pygaga.helpers.dbutils import get_db_engine, get_db

FLAGS = gflags.FLAGS

urls = (
    r'/', 'itemlist',
    r'/crawl/(.*)', 'crawlitem',
    r'/tbitem/(.*)', 'taobaoitem',
    r'/item/(.*)', 'item',
    r'/itemhtml/(.*).htm', 'itemhtml',
    r'/imagetile/(.*)', 'imagetile',
    r'/thumb/(.*)', 'thumb',
    r'/thumbview', 'thumbview',
    r'/products/(.*)', 'products',
)

IMAGE_PREFIX = 'http://image2.guang.j.cn/images/'
IMAGE_PATH = '/space/wwwroot/image.guang.j.cn/ROOT/'

class ItemImagesWrapper:
    def __init__(self, results):
        self.results = results

    def __iter__(self):
        for row in self.results:
            url = IMAGE_PREFIX + row['local_url']
            filename = IMAGE_PATH + "images/" + row['local_url']
            thumburl = '/thumb/' + row['local_url']
            image = None
            try:
                image = Image.open(cStringIO.StringIO(open(filename).read()))
            except:
                try:
                    image = Image.open(cStringIO.StringIO(urllib2.urlopen(url).read()))
                except:
                    traceback.print_exc()
            row.update({'itemurl':url, 'image':image, 'thumburl':thumburl})
            yield row

class itemlist:
    def GET(self):
        params = web.input()
        page = int(params.page) if hasattr(params, 'page') else 1
        page = max(1, page)
        perpage = int(params.perpage) if hasattr(params, 'perpage') else 100

        db = get_db()
        results = db.query('select distinct item_id, item.title, shop.name from item_images,item,shop where item_images.item_id=item.id and item.shop_id=shop.id limit %s offset %s' % (perpage, (page-1)*perpage))

        return render.itemlist(results, page, perpage)

class taobaoitem:
    def GET(self, id):
        db = get_db()
        results = db.query('select id from item where num_id=%s' % id)
        raise web.seeother('/item/%s' % results[0]['id'])

class crawlitem:
    def GET(self, id):
        db = get_db_engine()
        results = db.execute("select crawl_item_images.url, crawl_item_images.pos, crawl_item_images.type from crawl_html, crawl_item_images where crawl_item_images.item_id=crawl_html.item_id and crawl_html.item_id=%s;" % id)
        item_crawler = ItemCrawler(id, FLAGS.crawl_path)
        item_crawler.crawl(results, ((94,94), (350,350)), False)

        return render.crawlitem(id, item_crawler.results)

class item:
    def GET(self, id):
        params = web.input()
        page = int(params.page) if hasattr(params, 'page') else 1
        page = max(1, page)
        perpage = int(params.perpage) if hasattr(params, 'perpage') else 20
        try:
            itemid = int(id)
        except:
            itemid = -1

        db = get_db()
        if itemid >= 0:
            results = db.query("select item_images.id, item_id, url, local_url, type, pos, num_id, width, height, disabled, has_thumbnail from item_images,item where item_images.item_id=item.id and item_id=%s order by pos,type,disabled limit %s offset %s" % (itemid, perpage, (page-1)*perpage))

            crawl_results = db.query("select url, type, pos, is_crawled, err_reason from crawl_item_images where item_id=%s" % itemid)

            item_result = db.query('select item_id, num_id, detail_url, title, description, pic_url, local_pic_url, html, desc_content, promo_content from item,crawl_html where item.id=crawl_html.item_id and item.id=%s' % itemid)
            return render.itemdetail(id, ItemImagesWrapper(results), page, perpage, item_result[0])
        else:
            results = db.query("select item_id, url, local_url, type, num_id, width, height, disabled from item_images,item where item_images.item_id=item.id order by pos,type,disabled limit %s offset %s" % (perpage, (page-1)*perpage))
            return render.item(id, ItemImagesWrapper(results), page, perpage)

class itemhtml:
    def GET(self, id):
        db = get_db()
        item_result = db.query('select html, desc_content, promo_content from crawl_html where crawl_html.item_id=%s' % id)
        #web.header("Content-Type", "text/html; charset=utf-8")
        return item_result[0]['html']

class thumb:
    def GET(self, url):
        filename = IMAGE_PATH + "images/" + url
        httpurl = IMAGE_PREFIX + url
        try:
            try:
                image = Image.open(cStringIO.StringIO(open(filename).read()))
            except:
                try:
                    image = Image.open(cStringIO.StringIO(urllib2.urlopen(httpurl).read()))
                except:
                    traceback.print_exc()
            THUMBNAIL_SIZE = (350, 350)
            image.thumbnail(THUMBNAIL_SIZE, Image.ANTIALIAS)
            temp_handle = cStringIO.StringIO()
            image.save(temp_handle, 'png')
            temp_handle.seek(0)
        except:
            image = None
        web.header("Content-Type", "images/png")
        return temp_handle.read()

class imagetile:
    def GET(self, id):
        params = web.input()
        try:
            itemid = int(id)
        except:
            itemid = -1

        db = get_db()
        if itemid >= 0:
            results = db.query("select item_id, url, local_url, type, num_id, width, height, disabled from item_images,item where item_images.item_id=item.id and item_id=%s and disabled=0 order by item_images.id" % itemid)
        else:
            results = []
        return render.imagetile(id, ItemImagesWrapper(results))

class thumbview:
    def GET(self):
        params = web.input()
        if hasattr(params, "image_url"):
            return self.POST()
        else:
            return render.thumbview("", "", "", {}, "")

    def POST(self):
        params = web.input()
        url = params.image_url.strip()
        width = int(params.width)
        try:
            height = int(params.height)
        except:
            height = width
        image_filename = IMAGE_PATH + "/".join(url.split("/")[3:])
        thumb_path = IMAGE_PATH + "/images/thumb/"
        thumbs = {}

        try:
            image = Image.open(cStringIO.StringIO(open(image_filename).read()))

            THUMBNAIL_SIZE = (width, height)
            image.thumbnail(THUMBNAIL_SIZE, Image.ANTIALIAS)
            tmp_filename = os.tmpnam().replace("/", "_") + ".jpg"
            tmpname = thumb_path + tmp_filename
            tmpimg = open(tmpname, "w")
            image.save(tmpimg, 'jpeg')
            tmpimg.close()
            thumbs['pil'] = 'http://image2.guang.j.cn/images/thumb/' + tmp_filename
        except:
            traceback.print_exc()

        for x,y in [(0.2,0.3),(0.1,0.2),(0.4,0.5),(0.3,0.4)]:
            tmp_filename = os.tmpnam().replace("/", "_") + ".jpg"
            tmpname = thumb_path + tmp_filename
            os.system("convert +profile \"*\" -interlace Line -quality 95%% -resize %sx%s -sharpen %s,%s %s %s" % (width,height,x,y,image_filename, tmpname))
            thumbs['sharpen%s/%s' % (x,y)] = 'http://image2.guang.j.cn/images/thumb/' + tmp_filename

        if hasattr(params, "args"):
            args = params.args
            tmp_filename = os.tmpnam().replace("/", "_") + ".jpg"
            tmpname = thumb_path + tmp_filename
            os.system("convert %s %s %s" % (args,image_filename, tmpname))
            thumbs['args'] = 'http://image2.guang.j.cn/images/thumb/' + tmp_filename
        else:
            args = ""

        return render.thumbview(url, width, height, thumbs, args)

class products:
    IMG_RE = re.compile('(http://image[^/]*j.cn/images//[^/]+/)mid(/[a-zA-Z0-9_\.]+)')
    def GET(self, id):
        params = web.input()
        def fn(matchobj):
            org_url = matchobj.group(1) + 'big' + matchobj.group(2)
            image_filename = IMAGE_PATH + "/".join(org_url.split("/")[3:])
            thumb_path = IMAGE_PATH + "/images/thumb/"

            tmp_filename = os.tmpnam().replace("/", "_") + ".jpg"
            tmpname = thumb_path + tmp_filename

            if not hasattr(params, "type"):
                image = Image.open(cStringIO.StringIO(open(image_filename).read()))
                THUMBNAIL_SIZE = (210, 210)
                image.thumbnail(THUMBNAIL_SIZE, Image.ANTIALIAS)
                tmpimg = open(tmpname, "w")
                image.save(tmpimg, 'jpeg')
                tmpimg.close()
            elif params.type == '1':
                os.system("convert +profile \"*\" -interlace Line -quality 95%% -resize %sx%s -sharpen %s,%s %s %s" % (210,210,2,1,image_filename, tmpname))
            elif params.type == '2':
                os.system("convert +profile \"*\" -interlace Line -quality 95%% -thumbnail %sx%s -adaptive-sharpen %s,%s %s %s" % (210,210,2,1,image_filename, tmpname))

            return 'http://image2.guang.j.cn/images/thumb/' + tmp_filename
            #return org_url
        url = "http://www.j.cn/products/%s.htm" % id
        page = urllib2.urlopen(url).read()
        result = products.IMG_RE.subn(fn, page)
        return result[0]

app = web.application(urls, globals())
render = web.template.render('templates/', base='layout', globals={'prefix':web.prefixurl})

