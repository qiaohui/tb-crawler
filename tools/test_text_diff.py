#!/usr/bin/env python
# -*- coding: utf-8 -*

import re
from math import sqrt
import jieba

def file_reader():
    file_words = {}
    ignore_list = [u'的',u'了',u'和',u'呢',u'啊',u'哦',u'恩',u'嗯',u'吧']
    accepted_chars = re.compile(ur"[\u4E00-\u9FA5]+")

    all_the_text = "韩都衣舍韩版2013秋冬新款女装纯色连帽蝙蝠袖开衫毛衣JP2448琀"
    seg_list = jieba.cut(all_the_text, cut_all=True)
    #print "/".join(seg_list)
    for s in seg_list:
        if accepted_chars.match(s) and s not in ignore_list:
            if s not in file_words.keys():
                file_words[s] = [1, 0]
            else:
                file_words[s][0] += 1


    all_the_text = "韩都衣舍 韩国2013秋装新款女装纯色连帽蝙蝠袖开衫毛衣JP2448壹"
    seg_list = jieba.cut(all_the_text, cut_all=True)
    #print "/".join(seg_list)
    for s in seg_list:
        if accepted_chars.match(s) and s not in ignore_list:
            if s not in file_words.keys():
                file_words[s] = [0, 1]
            else:
                file_words[s][1] += 1

    sum_2 = 0
    sum_file1 = 0
    sum_file2 = 0
    for word in file_words.values():
        sum_2 += word[0] * word[1]
        sum_file1 += word[0] ** 2
        sum_file2 += word[1] ** 2

    rate = sum_2 / (sqrt(sum_file1 * sum_file2))
    print 'rate: ', rate



file_reader()

