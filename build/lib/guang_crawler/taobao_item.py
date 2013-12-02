#coding:utf8

import gflags
import logging
import traceback
import os
import Image
import cStringIO

from pygaga.helpers.urlutils import download
from pygaga.helpers.utils import make_dirs_for_file

logger = logging.getLogger('CrawlLogger')
FLAGS = gflags.FLAGS
DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"


class TaobaoItem:
    def __init__(self, shop_id, id, num_id):
        self.shop_id = shop_id
        self.id = id
        self.num_id = num_id

        self.title = ""
        self.detail_url = "" #detail_url.replace("spm=(\\.|\\d)*", "spm=2014.12669715.0.0")
        self.pic_url = ""
        self.local_pic_url = ""
        self.price = 0
        self.volume = 0
        self.status = 0
        self.campaign_id = 0
        self.uctrac_price = 0
        self.category = ""
        self.pic_width = 0
        self.pic_height = 0
        self.termIds = []

    def matchTaobaoTerms(self, term_factory, shop_termLimits):
        result = []
        if shop_termLimits:
            # 定义了terms limit，只在限定的term匹配
            termLimits = shop_termLimits.replace(" ", "")
            if termLimits:
                cids = [int(cid) for cid in termLimits.split(",")]
                toMatchTerms = []
                otherTerms = []
                for cid in cids:
                    sub_p_terms = term_factory.sub_terms[cid]
                    if len(sub_p_terms) > 0:
                        for cid, term in sub_p_terms.items():
                            if term.is_parent == 1:
                                continue
                            if term.is_other == 1:
                                otherTerms.append(term)
                                continue

                            toMatchTerms.append(term)

                for term in toMatchTerms:
                    if term.termRule.match(self):
                        result.append(term.cid)

                if len(result) == 0:
                    for term in otherTerms:
                        if term.termRule.match(self):
                            result.append(term.cid)

        else:
            # 没有定义term limits，全部TERM都要匹配
            for cid, term in term_factory.generic_terms.items():
                if term.termRule.match(self):
                    result.append(term.cid)

            if len(result) == 0:
                for cid, term in term_factory.other_terms.items():
                    if term.termRule.match(self):
                        result.append(term.cid)

        return result


    def setPicUrl(self, pic_url):
        self.pic_url = pic_url
        if self.local_pic_url:
            width, height = self.download_image(self.shop_id, self.num_id, self.pic_url, self.local_pic_url)
            self.width = width
            self.height = height
        else:
            self.local_pic_url = "%s_%s.%s" % (self.num_id, str(id(self)), pic_url.split('.')[-1].split('?')[0].split('/')[-1])
            width, height = self.download_image(self.shop_id, self.num_id, self.pic_url, self.local_pic_url)
            self.width = width
            self.height = height

    def setCampaign(self, defaultCampaign):
        self.campaign_id = defaultCampaign[0][0]
        self.uctrac_price = defaultCampaign[0][1]

    def download_image(self, shop_id, num_id, pic_url, local_pic_url):
        headers = {'Referer': "http://item.taobao.com/item.htm?id=%s" % num_id,
                   'User-Agent': DEFAULT_UA
        }
        big_path = "%s/%s/big/%s" % (FLAGS.path, shop_id, local_pic_url)
        mid2_path = "%s/%s/mid2/%s" % (FLAGS.path, shop_id, local_pic_url)
        mid_path = "%s/%s/mid/%s" % (FLAGS.path, shop_id, local_pic_url)
        sma_path = "%s/%s/sma/%s" % (FLAGS.path, shop_id, local_pic_url)
        small2_path = "%s/%s/small2/%s" % (FLAGS.path, shop_id, local_pic_url)
        small3_path = "%s/%s/small3/%s" % (FLAGS.path, shop_id, local_pic_url)
        try:
            data = download(pic_url, headers)
        except KeyboardInterrupt:
            raise
        except:
            logger.error("download %s:%s failed reason: %s", num_id, pic_url, traceback.format_exc())

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
                   "('%s','%s','%s',%s,%s,'%s',%s,'%s','%s',%s,%s,%s,%s,%s,now())" % (self.num_id, self.title, self.detail_url,
                                                                     self.price, self.shop_id, self.pic_url, self.status,
                                                                     self.local_pic_url, self.category, self.volume,
                                                                     self.campaign_id, self.uctrac_price,
                                                                     self.pic_height, self.pic_width)
        db.execute(item_sql)
        if self.termIds:
            # 2.create item_term
            item_id = int(db.execute("select id from item where num_id='%s' and shop_id=%s" % (self.num_id, self.shop_id)))
            if item_id:
                for term_id in self.termIds:
                    db.execute("insert into item_term (item_id, term_id, modify_date) values (%s, %s, now())" % (item_id, term_id))
            else:
                logger.error("create item failed, num_id=%s shop_id=%s" % (self.num_id, self.shop_id))

    def db_update(self, db):
        set_list = []
        if self.title:
            set_list.append(" title='%s'" % self.title)
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

