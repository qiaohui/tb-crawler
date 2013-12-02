#coding:utf8

import gflags
import logging
import os
import Image
import cStringIO
import re

from pygaga.helpers.urlutils import download
from pygaga.helpers.utils import make_dirs_for_file
from guang_crawler.taobao_html import TaobaoHtml
from pygaga.helpers.statsd import Statsd

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS

DEFAULT_UA="Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"

# 继承自TaobaoHtml
class TaobaoItem(TaobaoHtml):
    def __init__(self, shop_id, item_id, num_id):
        TaobaoHtml.__init__(self, item_id, num_id)
        self.shop_id = shop_id

        self.pic_url = ""
        self.local_pic_url = ""
        self.status = 0
        self.campaign_id = 0
        self.uctrac_price = 0
        self.category = ""
        self.pic_width = 0
        self.pic_height = 0
        self.termIds = []
        self.is_pic_download = True

    def matchTaobaoTerms(self, term_factory):
        result = []
        for cid, term in term_factory.all_terms.items():
            if term.termRule.match(self):
                result.append(term.cid)

        return result

    def setPicUrl(self):
        if len(self.thumbImages) > 0:
            tr = re.compile("(.*)_\d+x\d+\.jpg$")
            self.pic_url = tr.sub(r'\1', self.thumbImages[0])
            try:
                self.local_pic_url = str("%s_%s.%s" % (self.num_id, str(id(self)), self.pic_url.split('.')[-1].split('?')[0].split('/')[-1]))
                width, height = self.download_image()
                self.pic_width = width
                self.pic_height = height
            except:
                self.is_pic_download = False
                logger.error("item %s pic %s download failed", self.num_id, self.pic_url)
        else:
            self.is_pic_download = False
            logger.error("item %s not main pic", self.num_id)

    def setCampaign(self, defaultCampaign):
        self.campaign_id = defaultCampaign[0][0]
        self.uctrac_price = defaultCampaign[0][1]

    def download_image(self):
        headers = {
            'Referer': str(self.url),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding': 'gzip,deflate,sdch',
            'Accept-Language': 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3',
            'User-Agent': "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"
        }
        big_path = "%s/%s/big/%s" % (FLAGS.path, self.shop_id, self.local_pic_url)
        mid2_path = "%s/%s/mid2/%s" % (FLAGS.path, self.shop_id, self.local_pic_url)
        mid_path = "%s/%s/mid/%s" % (FLAGS.path, self.shop_id, self.local_pic_url)
        sma_path = "%s/%s/sma/%s" % (FLAGS.path, self.shop_id, self.local_pic_url)
        small2_path = "%s/%s/small2/%s" % (FLAGS.path, self.shop_id, self.local_pic_url)
        small3_path = "%s/%s/small3/%s" % (FLAGS.path, self.shop_id, self.local_pic_url)

        # 上层try/except, 便于stat
        data = download(self.pic_url, headers)
        if not data:
            time.sleep(2)
            data = download(self.pic_url, headers)
        self.save_image(big_path, data)

        self.imagemagick_resize(300, 300, big_path, mid2_path)
        self.imagemagick_resize(210, 210, big_path, mid_path)
        self.imagemagick_resize(60, 60, big_path, sma_path)
        self.imagemagick_resize(100, 100, big_path, small2_path)
        self.imagemagick_resize(145, 145, big_path, small3_path)

        return self.get_image_size(big_path)

    def imagemagick_resize(self, width, height, image_filename, thumb_filename):
        if not os.path.exists(os.path.dirname(thumb_filename)):
            make_dirs_for_file(thumb_filename)

        x = 0.3
        y = 0.4
        cmd = "convert +profile \"*\" -interlace Line -quality 95%% -resize %sx%s -sharpen %s,%s %s %s" % (
        width, height, x, y, image_filename, thumb_filename)
        os.system(cmd)

    def save_image(self, image_filename, data):
        if not os.path.exists(os.path.dirname(image_filename)):
            make_dirs_for_file(image_filename)
        f = file(image_filename, "wb")
        f.write(data)
        f.close()

    def get_image_size(self, image_filename):
        image = Image.open(cStringIO.StringIO(open(image_filename).read()))
        return image.size

    def db_create(self, db):
        # 1.create item
        item_sql = "insert into item(num_id,title,detail_url,price,shop_id,pic_url,status,local_pic_url," \
                   "category,volume,campaign_id,uctrac_price,pic_height,pic_width,created) values " \
                   "('%s','%s','%s',%s,%s,'%s',%s,'%s','%s',%s,%s,%s,%s,%s,now())" % (str(self.num_id), self.title.replace("'", "''").replace('%', '%%').replace('\\', ''), str(self.url),
                                                                     self.price, self.shop_id, self.pic_url, self.status,
                                                                     self.local_pic_url, self.category, self.volume,
                                                                     self.campaign_id, self.uctrac_price,
                                                                     self.pic_height, self.pic_width)
        db.execute(item_sql)
        item_id_list = list(db.execute("select id from item where num_id='%s' and shop_id=%s" % (self.num_id, self.shop_id)))

        if item_id_list:
            item_id = item_id_list[0][0]
            # 2.create item_term
            if self.termIds:
                for term_id in self.termIds:
                    db.execute("insert into item_term (item_id, term_id, modify_date) values (%s, %s, now())" % (item_id, term_id))

        else:
            logger.error("create item failed, num_id=%s shop_id=%s" % (self.num_id, self.shop_id))

    def db_update(self, db):
        set_list = []
        if self.title:
            set_list.append(" title='%s'" % self.title.replace("'", "''").replace('%', '%%').replace('\\', ''))
        if self.pic_url:
            set_list.append(" pic_url='%s'" % self.pic_url)
        if self.price != 0:
            set_list.append(" price=%s" % self.price)
        if self.volume != 0:
            set_list.append(" volume=%s" % self.volume)
        if self.category:
            set_list.append(" category='%s'" % self.category)
        if self.pic_width != 0:
            set_list.append(" pic_width=%s" % self.pic_width)
        if self.pic_height != 0:
            set_list.append(" pic_height=%s" % self.pic_height)
        if self.status != 0:
            set_list.append(" status=%s" % self.status)

        if len(set_list) > 0:
            set_list.append(" modified=now()")
            item_sql = "update item set %s where id = %s" % (",".join(set_list), self.id)
            db.execute(item_sql)

