#!/usr/bin/env python
# coding: utf-8

import gflags
import os
import logging
import sys

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

""" --> sql
CREATE TABLE `wb_qq_account` (
  `id` int(11) NOT NULL auto_increment,
  `name` varchar(255) NOT NULL default '' COMMENT '名字',
  `qqid` bigint(20) NOT NULL,
  `passwd` varchar(64) NOT NULL,
  `last_login` datetime default NULL COMMENT '上次登录成功时间',
  `status` int(11) NOT NULL default '1' COMMENT '1:正常;0:过期;',
  `cookies` text NULL,
  PRIMARY KEY  (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='QQ账号'
"""

QQID_CONFIG_PRODUCT = """
wwwjcn	2350678893
xuankuan4991	2483252460
jcnlingling	2480637221
jcnguozi	1478017837
jcnelva	2249984153
jcnxiaochen	1655874971
jcnqiangqiang	1028438958
jcnmanman1	1083250463
jcnxunzi	896174675
jcnkuner	1226492747
jcnxiaqing	2482309546
jcnsuxiaomo	2481751761
xuankuanshiCandy	1091422150
jcnkitty	2389425944
jcnruizi	2482191535
jcnyiyi	1719048093
jcnayumi	2480479657
jcnlanxin	2457942768
jcnevonne	2564818582
jcnjinglan	2684374095
jcnchris	1952818262
jcnmengzhu	194844460
"""

QQIDS = dict([l.strip().split() for l in QQID_CONFIG_PRODUCT.split("\n") if l])

def mig_main():
    db = get_db_engine()
    result = db.execute("select id,name,status from wb_account;")
    for row in result:
        sql = "update wb_qq_account set qqid=%s where name='%s'" % (QQIDS[row[1]], row[1])
        print sql
        db.execute(sql)
        #db.execute(sql, row[0], row[1], QQIDS[row[1]], 'langtaojin', row[2])


if __name__ == "__main__":
    log_init('CrawlLogger', "sqlalchemy.*")
    mig_main()
