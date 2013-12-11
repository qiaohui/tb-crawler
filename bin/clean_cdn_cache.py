import urllib2, urllib
import time

from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.logger import log_init

def refreshCdnCache(shop_id, local_pic_url):
    task = '{"urls":["http://image2.guang.j.cn/images/%s/mid2/%s"]}' % (shop_id, local_pic_url)
    data = {'username': 'langtaojin', 'password': 'LANGtaojin928', 'task': task}
    f = urllib2.urlopen(url='https://r.chinacache.com/content/refresh', data=urllib.urlencode(data))
    f.read()

    task = '{"urls":["http://image3.guang.j.cn/images/%s/mid2/%s"]}' % (shop_id, local_pic_url)
    data = {'username': 'langtaojin', 'password': 'LANGtaojin928', 'task': task}
    f = urllib2.urlopen(url='https://r.chinacache.com/content/refresh', data=urllib.urlencode(data))
    f.read()

    task = '{"urls":["http://image4.guang.j.cn/images/%s/mid2/%s"]}' % (shop_id, local_pic_url)
    data = {'username': 'langtaojin', 'password': 'LANGtaojin928', 'task': task}
    f = urllib2.urlopen(url='https://r.chinacache.com/content/refresh', data=urllib.urlencode(data))
    f.read()

    task = '{"urls":["http://image5.guang.j.cn/images/%s/mid2/%s"]}' % (shop_id, local_pic_url)
    data = {'username': 'langtaojin', 'password': 'LANGtaojin928', 'task': task}
    f = urllib2.urlopen(url='https://r.chinacache.com/content/refresh', data=urllib.urlencode(data))
    f.read()

    task = '{"urls":["http://image6.guang.j.cn/images/%s/mid2/%s"]}' % (shop_id, local_pic_url)
    data = {'username': 'langtaojin', 'password': 'LANGtaojin928', 'task': task}
    f = urllib2.urlopen(url='https://r.chinacache.com/content/refresh', data=urllib.urlencode(data))
    f.read()

def get_data():
    sql = "select shop_id,local_pic_url from item where modified>'2013-12-09 09' order by shop_id desc"
    db = get_db_engine()
    items = list(db.execute(sql))
    for item in items:
        refreshCdnCache(item[0], item[1])
        time.sleep(1)

if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    get_data()


