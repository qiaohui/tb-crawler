# coding: utf-8

import cStringIO
import hashlib
import logging
import os
import shutil
import time
import traceback
import urlparse
import urllib2
import Image

from fdfs_client.client import Fdfs_client

from pygaga.helpers.cachedns_urllib import custom_dns_opener
from pygaga.helpers.urlutils import download
from pygaga.helpers.statsd import Statsd

from guang_crawler.utils import gen_id

logger = logging.getLogger('CrawlLogger')

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
urllib2.install_opener(custom_dns_opener())

class ImageObj:
    def __init__(self, item_id, url, pos, image_type):
        self.item_id = item_id
        self.url = url
        self.generate_uniq_url()
        self.width = self.height = 0
        self.fullpath = ""
        self.filename = ""
        self.thumbs = {}
        self.pos = pos
        self.image_type = image_type
        self.disabled = True

    def get_static_url(self):
        return "/static/%s/%s" % (self.item_id, self.filename)

    def get_thumb_url(self, width, height):
        return "/static/%s/%sx%s/%s" % (self.item_id, width, height, self.filename)

    def get_server_path(self):
        return gen_id(self.item_id) + self.filename

    def get_server_thumb_path(self, thumbsize):
        return thumbsize + "_" + gen_id(self.item_id) + self.filename

    def generate_filename(self, crawl_tmp_path, ext):
        self.filename = "%s.%s" % (hashlib.md5(self.url).hexdigest(), ext)
        self.fullpath = "%s/%s" % (crawl_tmp_path, self.filename)

    def generate_uniq_url(self):
        urlobj = urlparse.urlparse(self.url)
        if urlobj.netloc.endswith('.taobaocdn.com'):
            self.uniq_url = "taobaocdn.com %s" % urlobj.path.split('/')[-1]
        elif urlobj.netloc.endswith('.firsteshop.cn'):
            self.uniq_url = "firsteshop.cn %s" % urlobj.path.split('/')[-1]
        elif urlobj.netloc.endswith('.freep.cn'):
            self.uniq_url = "freep.cn %s" % urlobj.path.split('/')[-1]
        else:
            self.uniq_url = "%s %s" % (urlobj.netloc, urlobj.path)

    def check_disabled(self):
        if (self.width < 300 or self.height < 150) and self.image_type != 1:
            self.disabled = True
        else:
            self.disabled = False

class ItemCrawler:
    def __init__(self, item_id, num_id, crawl_path, server_path=None, org_server_path=None, statshost=None, statsport=0):
        self.item_id = item_id
        self.results = {}
        self.headers = {
            'Referer': "http://item.taobao.com/item.htm?id=%s" % num_id,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
            'User-Agent': DEFAULT_UA
        }
        self.success = False
        self.crawl_path = crawl_path
        self.server_path = server_path
        self.org_server_path = org_server_path
        self.image_set = set()
        self.fdfs_client = Fdfs_client('/etc/fdfs/client.conf')
        self.statshost = statshost
        self.statsport = statsport

    def crawl(self, image_rows, thumb_sizes, is_commit=False, conn=None, is_remove=False):
        self.prepare_path(thumb_sizes)
        for item in image_rows:
            self.crawl_image(item, thumb_sizes, conn)
        self.check_success(thumb_sizes)
        if is_commit:
            if self.success:
                self.commit(conn)
            else:
                self.commit_fail(conn)
        if is_remove:
            try:
                shutil.rmtree(self.crawl_tmp_path)
            except:
                pass

    def prepare_path(self, thumb_sizes):
        self.crawl_tmp_path = "%s/%s" % (self.crawl_path, self.item_id)
        self.static_url = "static/%s" % self.item_id
        try:
            shutil.rmtree(self.crawl_tmp_path)
        except:
            pass

        try:
            os.makedirs(self.crawl_tmp_path)
        except:
            pass

        for width, height in thumb_sizes:
            try:
                os.makedirs("%s/%s/%sx%s" % (self.crawl_path, self.item_id, width, height))
            except:
                pass

    def check_success(self, thumb_sizes):
        self.success_type1_count = 0
        self.success_count = 0
        self.type1_count = 0
        self.count = 0
        has_type1_pos1 = False
        for pos, img in self.results.items():
            self.count += 1
            if img.image_type == 1:
                self.type1_count += 1
                if img.pos == 1:
                    has_type1_pos1 = True
            if not img.disabled and len(img.thumbs) == len(thumb_sizes):
                self.success_count += 1
                if img.image_type == 1:
                    self.success_type1_count += 1
        if self.success_type1_count > 0 and has_type1_pos1:
            self.success = True
        else:
            self.success = False
        self.summary = {'suc1':self.success_type1_count, 'count1':self.type1_count, 'suc':self.success_count, 'count':self.count}

    def commit_fail(self, conn):
        conn.execute("update crawl_html set is_image_crawled=0,last_modified=now(),crawl_image_info=\"%s\" where item_id=%s" % (self.summary, self.item_id))
        #conn.execute("update item set crawl_status=1 where id=%s" % self.item_id)

    def commit(self, conn):
        try:
            trans = conn.begin()
            conn.execute("delete from item_images where item_id=%s;" % self.item_id)
            for pos, img in self.results.items():
                if img.disabled or not img.url or not img.fullpath:
                    continue

                if len(img.thumbs) == 0:
                    continue

                if len(img.thumbs) > 1:
                    raise Exception("Thumbs size list len MUST BE ONE")

                for thumbsize, thumb_filename in img.thumbs.items():
                    #uploading to fastdfs
                    ret = self.fdfs_client.upload_by_filename(thumb_filename)
                    if ret['Status'] == 'Upload successed.':
                        group_name = ret['Group name']
                        remote_file = ret['Remote file_id']
                        # update database
                        conn.execute("insert into item_images (item_id,url,local_url,type,width,height,pos,is_downloaded,has_thumbnail,uniq_url, fastdfs_group, fastdfs_filename) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", self.item_id, img.url, img.get_server_path(), img.image_type, img.width, img.height, img.pos, 1, 1, img.uniq_url, group_name, remote_file)
                        logger.debug("upload to fastdfs %s -> %s", thumb_filename, remote_file)
                    else:
                        logger.warn("upload to fastdfs failed, reason: %s", ret)
                        raise Exception("upload failed %s" % ret)
            conn.execute("update crawl_html set is_image_crawled=1,crawl_image_info=\"%s\" where item_id=%s" % (self.summary, self.item_id))
            conn.execute("update item set crawl_status=2 where id=%s" % self.item_id)
            trans.commit()
        except Exception, e:
            logger.info("commit %s failed error: %s", self.item_id, traceback.format_exc())
            trans.rollback()
            raise

    def download_image(self, url):
        t = time.time()
        data = download(url, headers=self.headers)
        spent = time.time() - t
        Statsd.timing('guang.crawl.image', spent * 1000, host=self.statshost, port=self.statsport)
        return data

    def crawl_image(self, item, thumb_sizes, conn):
        url, pos, image_type = item
        url = url.encode('utf8') # TODO: should try gbk encoding?
        img = ImageObj(self.item_id, url, pos, image_type)
        if img.uniq_url in self.image_set:
            return
        else:
            self.image_set.add(img.uniq_url)
        logger.debug("crawling image %s %s %s", url, pos, image_type)
        self.results[pos] = img
        try:
            # TODO: find uniq path in db, if type=2 skip it
            # TODO: bloom filter this

            # downloading
            #data = self.get_downloaded_image(img, conn)
            #if not data:
            # TODO: user config black list
            if not self.is_allow_url(url):
                return

            data = self.download_image(url)

            self.save_image(url, data, img)
            img.check_disabled()
            if not img.disabled and data:
                self.generate_thumbs(img, thumb_sizes)
        except KeyboardInterrupt:
            raise
        except:
            logger.info("download %s:%s failed reason: %s", self.item_id, url, traceback.format_exc())

    def is_allow_url(self, url):
        host = urlparse.urlparse(url).netloc
        if host not in ["528371.138.user-website1.com"]:
            return True
        return False

    def save_image(self, url, data, img):
        if not data:
            return
        image = None
        try:
            image = Image.open(cStringIO.StringIO(data))
        except IOError, e:
            logger.info("Open image failed %s:%s %s", self.item_id, url, e.message)
        if not image:
            return
        if image.mode not in ('L', 'RGB'):
            image = image.convert('RGB')
        if image.format:
            if image.format.lower() == 'jpeg':
                ext = 'jpg'
            else:
                ext = image.format.lower()
        else:
            if image.info and image.info.has_key('version') and image.info['version'].lower().startswith('gif'):
                ext = 'gif'
            else:
                ext = url.split('.')[-1].split('?')[0].split('/')[-1]

        img.width, img.height = image.size

        img.generate_filename(self.crawl_tmp_path, ext)

        f = open(img.fullpath, "w")
        f.write(data)
        f.close()
        logger.debug("crawled image %s size %s %s", url, img.width, img.height)

    def generate_thumbs(self, img, thumb_sizes):
        logger.debug("generating thumbs %s : %s -- %sx%s -> %s", self.item_id, img.fullpath, img.width, img.height, thumb_sizes)
        # generate thumbs
        for width, height in thumb_sizes:
            thumb_filename = "%s/%sx%s/%s" % (self.crawl_tmp_path, width, height, img.fullpath.split("/")[-1])
            try:
                """
                image = Image.open(cStringIO.StringIO(open(img.fullpath).read()))
                if image.mode not in ('L', 'RGB'):
                    image = image.convert('RGB')
                if width != 710 :
                    image.thumbnail((width, height), Image.ANTIALIAS)
                else :
                    if img.width > 710 :
                        image.thumbnail((710, 710*img.height/img.width), Image.ANTIALIAS)
                thumbfile = open(thumb_filename, "w")
                image.save(thumbfile, "JPEG")
                thumbfile.close()
                """
                # 将上面的Image更换为imagemagick
                if width != 710:
                    convert_str = "convert -resize %sx%s -strip -quality 95 -density 72x72 %s %s" % (width, height, img.fullpath, thumb_filename)
                    os.system(convert_str)
                else:
                    if img.width > 710:
                        convert_str = "convert -resize %sx%s -strip -quality 95 -density 72x72 %s %s" % (710, 710*img.height/img.width, img.fullpath, thumb_filename)
                        os.system(convert_str)
                    else:
                        convert_str = "convert -resize %sx%s -strip -quality 95 -density 72x72 %s %s" % (img.width, img.height, img.fullpath, thumb_filename)
                        os.system(convert_str)
                img.thumbs["%sx%s" % (width, height)] = thumb_filename
            except:
                logger.info("generate thumb failed %s %s %sx%s error : %s", self.item_id, img.fullpath, width, height, traceback.format_exc())

    def get_downloaded_image(self, img, conn):
        if not conn:
            return None
        try:
            results = list(conn.execute("select local_url from item_images where uniq_url='%s'" % img.uniq_url))
            img_path = "%s%s" % (self.server_path, results[0][0])
            logger.debug("img exists, loading from %s" % img_path)
            return open(img_path, "rb").read()
        except:
            logger.debug("get download image failed: %s", traceback.format_exc())
            return None

