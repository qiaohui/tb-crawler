#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
import simplejson

from pandas import Series

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from pygaga.helpers.urlutils import download

gflags.DEFINE_string('solr_host', '192.168.10.78', 'solr host')

FLAGS = gflags.FLAGS

logger = logging.getLogger("statslogger")

SOLR_URL = "/solr/select?q=*%3A*&fq=item_id%3A%5B0+TO+*%5D&fq=term_parent_cid%3A3+OR+term_parent_cid%3A4+OR+term_parent_cid%3A5+OR+term_parent_cid%3A6+&fq=lctr_s2_2%3A*&start=0&rows=120&sort=lctr_s2_2+desc&wt=json&version=2"

def main():
    url = "http://%s:7080%s" % (FLAGS.solr_host, SOLR_URL)
    #import pdb; pdb.set_trace()
    results = simplejson.loads(download(url))
    db = get_db_engine()
    counts = []
    for doc in results['response']['docs']:
        item_id = doc['item_id']
        count = db.execute("select count(id) from favourite where itemid=%s and acttime>'2012-12-01' and favstatus=1 and firstchoose=0;" % item_id)
        if count.rowcount:
            counts.append(list(count)[0][0])
        else:
            counts.append(0)
    cs = Series(counts)
    logger.info(cs.describe())

if __name__ == '__main__':
    log_init(['statslogger','urlutils'], "sqlalchemy.*")
    main()
