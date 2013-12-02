#!/usr/bin/env python
# coding: utf-8

import gflags
import logging
import sys
import re

from pygaga.helpers.logger import log_init

from pygaga.helpers.dbutils import get_db_engine

gflags.DEFINE_boolean('dryrun', False, "commit?")

logger = logging.getLogger('CrawlLogger')

FLAGS = gflags.FLAGS

def fix_url_main():
    SPMRE = re.compile("^(.*spm=)([^&]*)(.*)$")
    db = get_db_engine()
    #results = list(db.execute("select id,detail_url from item where detail_url like 'http://s.click.taobao.com/%%';"))
    results = list(db.execute("select id,detail_url from item where detail_url like '%%.taobao.com/%%';"))
    for r in results:
        id = r[0]
        url = r[1]
        if url.find('spm=') > 0:
            url = SPMRE.subn(r'\g<1>2014.12669715.0.0\g<3>', url)[0]
        else:
            url = url + '&spm=2014.12669715.0.0'
        sql = "update item set detail_url = '%s' where id=%s" % (url.replace('%', '%%'), id)
        #if url.find("_UCTRAC_CLK_") > 0:
        #    continue
        #sql = "update item set detail_url='%s&u=jn_UCTRAC_CLK_' where id = %s" % (url.replace('%','%%'), id)
        logger.debug(sql)
        if not FLAGS.dryrun:
            db.execute(sql)

if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    fix_url_main()

