# coding: utf-8

PREFIX = "itemimg"

DEFAULT_UA = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"

def gen_id(item_id):
    first = item_id / 1000 / 1000
    second = (item_id - first * 1000 * 1000) / 1000
    third = item_id - first * 1000 * 1000 - second * 1000
    return "%s/%s/%s/%s/" % (PREFIX, first, second, third)

