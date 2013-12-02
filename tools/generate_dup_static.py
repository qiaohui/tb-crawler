#!/usr/bin/env python
# coding: utf-8
import random
import gflags
import logging
from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

logger = logging.getLogger('AppLogger')
FLAGS = gflags.FLAGS
gflags.DEFINE_string('file', '../result.txt', "result file")

def remove():
    db = get_db_engine()
    content = open(FLAGS.file)
    j = 0
    for l in content:
        key = l.split('\t')
        print("querying %s", key[0])
        result = db.execute("select id from item_images where uniq_url=%s and disabled=0", key[0])
        i = 0
        j+=1
        for r in result:
            sql = "update item_images set disabled=1 where id=%s" % r[0]
            print("deleting %s %s/%s %s", j, i, result.rowcount, sql)
            db.execute(sql)
            i+=1

def save():
    db = get_db_engine()
    content = open(FLAGS.file)
    html = '<html><body>'
    for l in content:
        key = l.split('\t')
        result = db.execute("select fastdfs_filename from item_images where uniq_url=%s limit 10", key[0])
        if result.rowcount > 0:
            html += '<div>'
            #for r in result:
            #    html += '<p><img src="http://img%s.guang.j.cn/%s"></p>' % (random.randint(1,5), r[0])
            html += '<p><img src="http://img%s.guang.j.cn/%s"></p>' % (random.randint(1,5), list(result)[0][0])
            html += '</div>'
    html += '</body></html>'
    print html

def main():
    remove()

if __name__ == "__main__":
    log_init('AppLogger', "sqlalchemy.*")
    main()
