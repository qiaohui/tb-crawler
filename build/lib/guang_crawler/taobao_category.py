#! /usr/bin/env python
#coding:utf8

import gflags

from guang_crawler.taobao_api import get_taobao_itemcats, get_top

FLAGS = gflags.FLAGS

class TaobaoCategory:
    def __init__(self, db):
        self.db = db
        self.categories_list = self.loadCategorys()
        self.categories_dict = {}
        self.tree_to_map(self.categories_list)

    def tree_to_map(self, categories_list):
        #cid,pid,name
        cp = {}
        cn = {}
        for c in categories_list:
            cp[c[0]] = c[1]
            cn[c[0]] = c[2]
        
        for k in cp:
            cm = []
            kk = k
            while kk != 0:
                if kk in cn:
                    cm.append(cn[kk])
                    kk = cp[kk]
                elif kk in self.categories_dict:
                    cm.append(self.categories_dict[kk])
                    kk = self.categories_dict[kk][1]
                else:
                    break
            self.categories_dict[k] = (cm, cp[k])

    def loadCategorys(self):
        return list(self.db.execute("select cid, pid, name from tb_category"))

    def buildPath(self, cid):
        itemcats = get_taobao_itemcats(get_top(), cid)
        path = []
        if itemcats:
            name = itemcats['item_cats']['item_cat'][0]['name']
            parent_cid = itemcats['item_cats']['item_cat'][0]['parent_cid']

            self.saveCategory(cid, parent_cid, name)

            if parent_cid != 0:
                # 注意这里有迭代
                path.append(self.buildPath(parent_cid))

        return ",".join(path)

    def saveCategory(self, cid, parent_cid, name):
        self.tree_to_map([(cid, parent_cid, name)])
        self.db.execute("insert into tb_category values (%s,%s,%s)", cid, parent_cid, name)

    def getCategoryPath(self, cid):
        if self.categories_dict.has_key(long(cid)):
            cnames = self.categories_dict[long(cid)][0]
            return ",".join(cnames)
        else:
            return self.buildPath(cid)


if __name__ == "__main__":
    from pygaga.helpers.logger import log_init
    log_init("CrawlLogger", "sqlalchemy.*")
    from pygaga.helpers.dbutils import get_db_engine
    db = get_db_engine()
    category = TaobaoCategory(db)
    for cid in ['50124001', '50104003', '50454031']:
        print category.getCategoryPath(cid)

