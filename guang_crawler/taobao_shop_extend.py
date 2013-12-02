#! /usr/bin/env python
#coding:utf8

import gflags
import logging
import datetime


from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine

FLAGS = gflags.FLAGS
logger = logging.getLogger('CrawlLogger')

class ShopExtendInfo:
    '''店铺扩展信息 持久化类'''

    def __init__(self, db, shop_id):
        self.db = db
        self.__id = 0

        if not shop_id:
            raise Exception('ShopExtendInfo init error: no shop_id.')

        self.__shop_id = shop_id
        self.main_category = ''
        self.location = ''
        self.good_item_rate = 0.0
        self.described_remark = 0.0
        self.described_remark_compare = 0.0
        self.service_remark = 0.0
        self.service_remark_compare = 0.0
        self.shipping_remark = 0.0
        self.shipping_remark_compare = 0.0
        self.support_returnin7day = 0
        self.support_cash = 0
        self.support_consumer_guarantees = 0
        self.support_credit_card = 0
        self.open_at = datetime.datetime(1900, 1, 1)
        self.create_at = None
        self.modify_at = None
        self.favorited_user_count = 0

        self.get()

    def exists(self):
        """判断是否已经在数据库中存在"""
        return self.__id > 0

    def get(self):
        data = self.db.execute(\
"SELECT id, shop_id, main_category, location, good_item_rate, described_remark, \
described_remark_compare, service_remark, service_remark_compare, shipping_remark, \
shipping_remark_compare, support_returnin7day, support_cash, support_consumer_guarantees, \
support_credit_card, open_at, create_at, modify_at, favorited_user_count \
FROM shop_extend where shop_id ='%s' limit 1;", self.__shop_id)
        if data.rowcount > 0:
            record = list(data)
            self.__id = record[0][0]
            self.__shop_id = record[0][1]
            self.main_category = record[0][2].encode('utf-8')
            self.location = record[0][3].encode('utf-8')
            self.good_item_rate = float(record[0][4])
            self.described_remark = float(record[0][5])
            self.described_remark_compare = float(record[0][6])
            self.service_remark = float(record[0][7])
            self.service_remark_compare = float(record[0][8])
            self.shipping_remark = float(record[0][9])
            self.shipping_remark_compare = float(record[0][10])
            self.support_returnin7day = record[0][11]
            self.support_cash = record[0][12]
            self.support_consumer_guarantees = record[0][13]
            self.support_credit_card = record[0][14]
            self.open_at = record[0][15]
            self.create_at = record[0][16]
            self.modify_at = record[0][17]
            self.favorited_user_count = record[0][18]

    def get_shop_id(self):
        return self.__shop_id

    def get_id(self):
        return self.__id

    def update(self):

        if not self.exists:
            raise Exception('ShopExtendInfo update error: not exist.')

        set_list = []
        value_list = []

        set_list.append(" main_category=%s")
        value_list.append(self.main_category)

        set_list.append(" location=%s")
        value_list.append(self.location)

        set_list.append(" good_item_rate=%s")
        value_list.append("%3.2f" % self.good_item_rate)

        set_list.append(" described_remark=%s")
        value_list.append("%1.1f" % self.described_remark)

        set_list.append(" described_remark_compare=%s")
        value_list.append("%3.2f" % self.described_remark_compare)

        set_list.append(" service_remark=%s")
        value_list.append("%1.1f" % self.service_remark)

        set_list.append(" service_remark_compare=%s")
        value_list.append("%3.2f" % self.service_remark_compare)

        set_list.append(" shipping_remark=%s")
        value_list.append("%1.1f" % self.shipping_remark)

        set_list.append(" shipping_remark_compare=%s")
        value_list.append("%3.2f" % self.shipping_remark_compare)

        set_list.append(" support_returnin7day=%s")
        value_list.append(self.support_returnin7day)

        set_list.append(" support_cash=%s")
        value_list.append(self.support_cash)

        set_list.append(" support_consumer_guarantees=%s")
        value_list.append(self.support_consumer_guarantees)

        set_list.append(" support_credit_card=%s")
        value_list.append(self.support_credit_card)

        set_list.append(" favorited_user_count=%s")
        value_list.append(self.favorited_user_count)

        set_list.append(" open_at=%s")
        value_list.append(self.open_at.strftime('%Y-%m-%d'))

        set_list.append(" modify_at=now()")
        item_sql = "update shop_extend set %s where shop_id = %s ;" % (",".join(set_list), self.__shop_id)
        self.db.execute(item_sql, value_list)

    def insert(self):
        if self.exists():
            raise Exception('ShopExtendInfo insert error: exists the record with the same shop_id.')
        value_list = []
        value_list.append("%d" % self.__shop_id)
        value_list.append("%s" % self.main_category)
        value_list.append("%s" % self.location)
        value_list.append("%3.2f" % self.good_item_rate)
        value_list.append("%1.1f" % self.described_remark)
        value_list.append("%3.2f" % self.described_remark_compare)
        value_list.append("%1.1f" % self.service_remark)
        value_list.append("%3.2f" % self.service_remark_compare)
        value_list.append("%1.1f" % self.shipping_remark)
        value_list.append("%3.2f" % self.shipping_remark_compare)
        value_list.append("%d" % self.support_returnin7day)
        value_list.append("%d" % self.support_cash)
        value_list.append("%d" % self.support_consumer_guarantees)
        value_list.append("%d" % self.support_credit_card)
        value_list.append("%d" % self.favorited_user_count)
        value_list.append("%s" % self.open_at.strftime('%Y-%m-%d'))

        item_sql = '''INSERT INTO shop_extend
(shop_id,main_category,location,good_item_rate,described_remark,
described_remark_compare,service_remark,service_remark_compare,
shipping_remark,shipping_remark_compare,support_returnin7day,
support_cash,support_consumer_guarantees,support_credit_card,
favorited_user_count,open_at,create_at,modify_at)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now());'''

        self.db.execute(item_sql, value_list)

    def save(self):
        if self.exists():
            self.update()
        else:
            self.insert()


if __name__ == "__main__":

    def assert_equal(got, expected):
        if got == expected:
            prefix = ' OK '
        else:
            prefix = '  X '
            raise Exception('%s got: %s expected: %s' % (prefix, repr(got), repr(expected)))
        print '%s got: %s expected: %s' % (prefix, repr(got), repr(expected))

    def test():
        log_init("CrawlLogger", "sqlalchemy.*")
        db = get_db_engine()

        #测试新建
        theshop = ShopExtendInfo(db, 10000001)
        theshop.main_category = "服饰箱包"
        theshop.location = "浙江杭州"
        theshop.good_item_rate = 99.98
        theshop.described_remark = 4.8
        theshop.described_remark_compare = 32.13
        theshop.service_remark = 4.6
        theshop.service_remark_compare = -15.20
        theshop.support_returnin7day = 0
        theshop.support_cash = 1
        theshop.support_consumer_guarantees = 1
        theshop.support_credit_card = 0
        theshop.open_at = datetime.datetime.strptime("2013-1-1", "%Y-%m-%d")
        theshop.favorited_user_count = 1234567890
        theshop.save()

        testtheshop = ShopExtendInfo(db, 10000001)
        assert_equal(testtheshop.main_category, "服饰箱包")
        assert_equal(testtheshop.location, "浙江杭州")
        assert_equal(testtheshop.good_item_rate, 99.98)
        assert_equal(testtheshop.described_remark, 4.8)
        assert_equal(testtheshop.described_remark_compare, 32.13)
        assert_equal(testtheshop.service_remark, 4.6)
        assert_equal(testtheshop.service_remark_compare, -15.20)
        assert_equal(testtheshop.support_returnin7day, 0)
        assert_equal(testtheshop.support_cash, 1)
        assert_equal(testtheshop.support_consumer_guarantees, 1)
        assert_equal(testtheshop.support_credit_card, 0)
        assert_equal(testtheshop.open_at, datetime.date(2013, 1, 1))
        assert_equal(testtheshop.favorited_user_count, 1234567890)

        #测试修改部分
        theshop = ShopExtendInfo(db, 10000001)
        theshop.main_category = "服饰箱包TEST"
        theshop.location = "浙江杭州TEST"
        theshop.good_item_rate = 10.98
        theshop.described_remark = 3.8
        theshop.described_remark_compare = -32.13
        theshop.service_remark = 3.6
        theshop.save()

        testtheshop = ShopExtendInfo(db, 10000001)
        assert_equal(testtheshop.main_category, "服饰箱包TEST")
        assert_equal(testtheshop.location, "浙江杭州TEST")
        assert_equal(testtheshop.good_item_rate, 10.98)
        assert_equal(testtheshop.described_remark, 3.8)
        assert_equal(testtheshop.described_remark_compare, -32.13)
        assert_equal(testtheshop.service_remark, 3.6)

        #测试修改全部
        theshop = ShopExtendInfo(db, 10000001)
        theshop.main_category = "服饰箱包Test2"
        theshop.location = "浙江杭州Test2"
        theshop.good_item_rate = 13.98
        theshop.described_remark = 4.7
        theshop.described_remark_compare = 10.13
        theshop.service_remark = 4.8
        theshop.service_remark_compare = -12.20
        theshop.support_returnin7day = 1
        theshop.support_cash = 1
        theshop.support_consumer_guarantees = 0
        theshop.support_credit_card = 1
        theshop.open_at = datetime.datetime.strptime("2013-2-1", "%Y-%m-%d")
        theshop.favorited_user_count = 1234567891
        theshop.save()

        testtheshop = ShopExtendInfo(db, 10000001)
        assert_equal(testtheshop.main_category, "服饰箱包Test2")
        assert_equal(testtheshop.location, "浙江杭州Test2")
        assert_equal(testtheshop.good_item_rate, 13.98)
        assert_equal(testtheshop.described_remark, 4.7)
        assert_equal(testtheshop.described_remark_compare, 10.13)
        assert_equal(testtheshop.service_remark, 4.8)
        assert_equal(testtheshop.service_remark_compare, -12.20)
        assert_equal(testtheshop.support_returnin7day, 1)
        assert_equal(testtheshop.support_cash, 1)
        assert_equal(testtheshop.support_consumer_guarantees, 0)
        assert_equal(testtheshop.support_credit_card, 1)
        assert_equal(testtheshop.open_at, datetime.date(2013, 2, 1))
        assert_equal(testtheshop.favorited_user_count, 1234567891)

                #测试修改全部
        theshop = ShopExtendInfo(db, 10000001)
        theshop.main_category = "服饰箱包Test2"
        theshop.location = "浙江杭州Test2"
        theshop.good_item_rate = 100.00
        theshop.described_remark = 4.7
        theshop.described_remark_compare = 100.00
        theshop.service_remark = 4.8
        theshop.service_remark_compare = -100.00
        theshop.shipping_remark = 5.0
        theshop.shipping_remark_compare = -100.00
        theshop.support_returnin7day = 1
        theshop.support_cash = 1
        theshop.support_consumer_guarantees = 0
        theshop.support_credit_card = 1
        theshop.open_at = datetime.datetime.strptime("2013-2-1", "%Y-%m-%d")
        theshop.favorited_user_count = 1234567891
        theshop.save()

        testtheshop = ShopExtendInfo(db, 10000001)
        assert_equal(testtheshop.main_category, "服饰箱包Test2")
        assert_equal(testtheshop.location, "浙江杭州Test2")
        assert_equal(testtheshop.good_item_rate, 100.00)
        assert_equal(testtheshop.described_remark, 4.7)
        assert_equal(testtheshop.described_remark_compare, 100.00)
        assert_equal(testtheshop.service_remark, 4.8)
        assert_equal(testtheshop.service_remark_compare, -100.00)
        assert_equal(testtheshop.shipping_remark, 5.0)
        assert_equal(testtheshop.shipping_remark_compare, -100.00)
        assert_equal(testtheshop.support_returnin7day, 1)
        assert_equal(testtheshop.support_cash, 1)
        assert_equal(testtheshop.support_consumer_guarantees, 0)
        assert_equal(testtheshop.support_credit_card, 1)
        assert_equal(testtheshop.open_at, datetime.date(2013, 2, 1))
        assert_equal(testtheshop.favorited_user_count, 1234567891)

    test()




