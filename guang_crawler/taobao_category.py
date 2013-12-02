#! /usr/bin/env python
#coding:utf8

import gflags
import logging

from guang_crawler.taobao_api import get_taobao_itemcats, get_top
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

FLAGS = gflags.FLAGS
logger = logging.getLogger('CrawlLogger')

class TaobaoCategory:
    def __init__(self, db):
        self.db = db
        self.categories_list = self.loadCategorys()
        self.categories_dict = {}
        # init
        self.tree_to_map(self.categories_list)

    def tree_to_map(self, categories_list):
        #cid,pid,name
        cp = {}
        cn = {}
        for c in categories_list:
            cp[c[0]] = c[1]
            cn[c[0]] = c[2].encode('utf-8')
        
        for cid in cp:
            temp = []
            pid = cid
            while pid != 0:
                if pid in cn:
                    temp.append(cn[pid])
                    pid = cp[pid]
                elif pid in self.categories_dict:
                    temp.append(",".join(self.categories_dict[pid][0]))
                    pid = self.categories_dict[pid][1]
                else:
                    break
            self.categories_dict[cid] = (temp, cp[cid])

    def loadCategorys(self):
        return list(self.db.execute("select cid, pid, name from tb_category"))

    def buildPath(self, cid):
        itemcats = get_taobao_itemcats(get_top(), cid)
        path = []
        if itemcats:
            name = itemcats['item_cats']['item_cat'][0]['name']
            parent_cid = itemcats['item_cats']['item_cat'][0]['parent_cid']

            self.saveCategory(cid, parent_cid, name)

            path.append(name.encode("utf-8"))
            if parent_cid != 0:
                # 注意这里有迭代
                path.append(self.buildPath(parent_cid))

        return ",".join(path)

    def saveCategory(self, cid, parent_cid, name):
        self.tree_to_map([(cid, parent_cid, name)])
        categories = list(self.db.execute("select cid, pid, name from tb_category where cid=%s", cid))
        if not categories:
            self.db.execute("insert into tb_category values (%s,%s,%s)", cid, parent_cid, name)

    def getCategoryPath(self, cid):
        if self.categories_dict.has_key(long(cid)):
            cnames = self.categories_dict[long(cid)][0]
            return ",".join(cnames)
        else:
            return self.buildPath(cid)


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    db = get_db_engine()
    category = TaobaoCategory(db)
    for cid in ['50013876', '50124001', '50104003', '50454031']:
        print category.getCategoryPath(cid)

