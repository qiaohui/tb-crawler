#coding:utf8

import os
import sys
import logging
import urllib
from solar import SolrSearcher, X

logging.basicConfig(level=logging.DEBUG)

def T(input):
    if os.environ.get('TM_DISPLAYNAME', ''):
        return input
    else:
        return input.encode('utf8')

searcher = SolrSearcher('http://sdl-guang-solr2:7080/solr')
#searcher = SolrSearcher('http://127.0.0.1:8983/solr')
#searcher = SolrSearcher('http://211.100.61.27:7080/solr')

def get_items():
    #q = searcher.search(u'韩').edismax().qf([('item_title', 1.4), ('shop_name', 0.5)]).set_param("debugQuery", "true").limit(60).set_param('bf', 'sum(mul(yantao_ctr6,20.0),mul(tagdist(guang_tag_match,1:0.5,3:0.5),0.3))')

    #q = searcher.search(u'格子衬衫').edismax().qf([('item_title', 1.4), ('shop_name', 0.5)]).set_param("debugQuery", "true").limit(60).set_param('bf', 'mul(yantao_ctr6,1.6)')
    #q = searcher.search(u'荧光 AND 鞋').edismax().qf([('item_title', 1.4), ('shop_name', 0.5)]).set_param("debugQuery", "true").limit(120).set_param('bf', 'mul(yantao_ctr6,1.6)')
    q = searcher.search(u'白色').edismax().qf([('item_title', 1.4), ('shop_name', 0.5)]).set_param("debugQuery", "true").limit(120).set_param('bf', 'mul(score,10)')
    #q = searcher.search(u'白色').edismax().qf([('item_title', 1.4), ('shop_name', 0.5)]).set_param("debugQuery", "true").limit(120).set_param('bf', 'sum(mul(yantao_ctr5,1.5),mul(tagdist(guang_tag_match,1:0.5,3:0.5),15.0))')
    #q = searcher.search(u'白色').edismax().qf([('item_title', 1.4), ('shop_name', 0.5)]).set_param("debugQuery", "true").limit(120).set_param('bf', 'sum(mul(yantao_ctr5,1.5),mul(tagdist(guang_tag_match,2:0.5,4:0.5),15.0))')

    ##q = searcher.search(item_title=u'格子衬衫', shop_name=u'格子衬衫', _op=X.OR).offset(1).limit(30).set_param("debugQuery", "true").only("*", "score", "[docid]")
    for i in q:
        try:
            print "%s %s %s %s %s %s " % (i.item_id, i.score, i.yantao_ctr5, T(i.item_title), '--', T(i.shop_name))
        except:
            print "%s %s %s %s %s %s " % (i.item_id, i.score, None, T(i.item_title), '--', T(i.shop_name))

    debug_info = q.results.debug_info
    print 'QParser', T(debug_info['QParser'])
    print 'querystring', T(debug_info['querystring'])
    print 'parsedquery', T(debug_info['parsedquery'])
    print 'explain', T(debug_info['explain'][u''])
    for k in debug_info['timing']:
        print 'timing_%s' % k, debug_info['timing'][k]

def get_all_shops():
    q = searcher.search().filter(guang_shop_id__gte="0").limit(10).set_param("debugQuery", "true")

    for i in q:
        print i.guang_shop_id, T(i.guang_shop_name)

def get_all_terms():
    q = searcher.search().filter(guang_term_id__gte="0").limit(10).set_param("debugQuery", "true")

    for i in q:
        print i.guang_term_id[0], T(i.guang_term_name[0]), i.guang_term_name

#get_all_shops()
#get_all_terms()
get_items()
