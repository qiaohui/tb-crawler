#!/Library/Frameworks/Python.framework/Versions/2.7/Resources/Python.app/Contents/MacOS/Python
# coding: utf-8

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engines

from guang_crawler.taobao_api import get_taobao_items, get_top

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

gflags.DEFINE_integer('limit', 50000, "Batch counts")
gflags.DEFINE_boolean('commit_price', False, "is commit price into db")
gflags.DEFINE_list('xdbconnstrs', ["guang:guang@192.168.32.10:3306/guang?charset=utf8","guang:guang@192.168.33.161:3306/guang?charset=utf8"], "database connstrs")

def crawl_main():
    write_db, read_db = get_db_engines(**{'dbconnstrs' : FLAGS.xdbconnstrs})

    sql = "select item.id, item.num_id, item.price, item.pic_url, item.volume from item_hotest, item, shop where item_hotest.item_id = item.id and item.status = 1 and item.shop_id = shop.id and shop.type <= 2 and shop.status=1 limit %s" % FLAGS.limit

    rows = read_db.execute(sql)

    counter = 0
    off_counter = 0
    change_counter = 0
    vol_change_counter = 0
    total = rows.rowcount
    results = get_taobao_items(get_top(), rows, fn_join_iids=lambda x:','.join([str(i[1]) for i in x]), calllimit=300)
    for batch_item in results:
        for iid, item in batch_item.items.items():
            try:
                counter += 1
                item_id = item['req'][0]
                item_iid = item['req'][1]
                item_price = item['req'][2]
                #item_picurl = item['req'][3]
                if item['resp']:
                    if item['resp']['approve_status'] != 'onsale':
                        logger.debug("Item %s/%s %s %s is offshelf", counter, total, item_id, item_iid)
                        off_counter += 1
                        write_db.execute("update item set status=2, modified=now()  where id=%s" % item_id)
                    else:
                        price = float(item['resp']['price'])
                        #title = item['resp']['title']
                        #pic_url = item['resp']['pic_url']
                        if abs(item_price - price) / (item_price + 0.0000001) > 0.2 or abs(item_price - price) > 2.0:
                            change_counter += 1
                            logger.debug("Item %s/%s %s %s price %s -> %s", counter, total, item_id, item_iid, item_price, price)
                            if FLAGS.commit_price:
                                write_db.execute("update item set price=%s where id=%s" % (price, item_id))
                logger.debug("req %s resp %s", item['req'], item['resp'])
            except:
                logger.error("update failed %s", traceback.format_exc())
    logger.info("Taobao quickupdate, total %s, off %s, price change %s, volume change %s", total, off_counter, change_counter, vol_change_counter)

if __name__ == "__main__":
    log_init(['CrawlLogger', 'TaobaoLogger',], "sqlalchemy.*")
    crawl_main()
