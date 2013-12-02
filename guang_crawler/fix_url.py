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
    #log_init('CrawlLogger', "sqlalchemy.*")
    fix_url_main()

    """
    # 淘宝客链接转成正常链接
    import requests, urllib
    #url="http://s.click.taobao.com/t?e=m%3D2%26s%3DAPGxOyU70rEcQipKwQzePOeEDrYVVa64XoO8tOebS%2BdRAdhuF14FMTsnblrvEjqEJ1gyddu7kN%2BhAh3HOJNdaNQRGlh44dSUTkt1PB7EAQYTzSJ6RGKXv9AqgaNS7oS9sD436C2dwG2qDVoHcSt9RL4lut2PUnyd&spm=2014.12669715.1.0&u=jn_UCTRAC_CLK_&unid=jn_UCTRAC_CLK_"
    url = "http://s.click.taobao.com/t?e=m%3D2%26s%3DugdNkm95jWQcQipKwQzePOeEDrYVVa64yK8Cckff7TVRAdhuF14FMTuvK%2FEG4bz879%2FTFaMDK6ShAh3HOJNdaNQRGlh44dSUTkt1PB7EAQYTzSJ6RGKXvzJwXfj%2B8jZGakjN3kYacdMBhe31NbCo54h0323Wskzb&spm=2014.12669715.1.0&u=jn_UCTRAC_CLK_&unid=jn_UCTRAC_CLK_"
    def get_real_taobao(url):
        _refer = requests.get(url).url
        headers = {'Referer': _refer}
        return requests.get(urllib.unquote(_refer.split('tu=')[1]), headers=headers).url
    rel = get_real_taobao(url)
    print rel
    """

