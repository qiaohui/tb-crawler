#coding:utf8

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from taobao_item import TaobaoItem

FLAGS = gflags.FLAGS
logger = logging.getLogger('CrawlLogger')

class TermFactory:
    def __init__(self, db):
        self.db = db
        self.all_terms = {}     # {cid:term}
        self.other_terms = {}   # {cid:term}
        self.generic_terms = {} # {cid:term}
        self.sub_terms = {}     # {cid: {p_cid:term}}

        # init term data
        self.loadTerms()


    def loadTerms(self):
        terms_list = list(self.db.execute("select cid, parent_cid, is_parent, name, rule, is_other from term where status = 0"))
        for term_obj in terms_list:
            term = Term(term_obj[0], term_obj[1], term_obj[2], term_obj[3], term_obj[4], term_obj[5])
            self.all_terms[term.cid] = term
            if term.is_parent == 0 and term.is_other == 0:
                self.generic_terms[term.cid] = term
            elif term.is_parent == 0 and term.is_other == 1:
                self.other_terms[term.cid] = term
        # 获取自己的父类，然后再获取自己的2层子类
        for cid, term in self.all_terms.items():
            p_terms = {}
            p_terms[cid] = term
            p_cid = term.parent_cid
            # parent
            while p_cid > 0:
                p_terms[p_cid] = self.all_terms[p_cid]
                p_cid = self.all_terms[p_cid].parent_cid
            # leaf，这里用了比较傻瓜的方式，有没有更好的算法???
            for l_cid, l_term in self.all_terms.items():
                if l_term.parent_cid == cid:
                    p_terms[l_cid] = l_term
                    for l_l_cid, l_l_term in self.all_terms.items():
                        if l_l_term.parent_cid == l_cid:
                            p_terms[l_l_cid] = l_l_term
            
            self.sub_terms[cid] = p_terms

class Term:
    def __init__(self, cid, parent_cid, is_parent, name, rule, is_other):
        self.cid = cid
        self.parent_cid = parent_cid
        self.name = name
        self.rule = rule
        self.is_parent = is_parent
        self.is_other = is_other
        # init termRule
        self.termRule = TermRule(self)


class TermRule:
    def __init__(self, term):
        self.postFixExp = []
        self.buildRule(term)

    def buildRule(self, term):
        """
            Term存放了中缀表达式的规则，需要转换成后缀表达式
        """
        if type(term.rule) != unicode or type(term.name) != unicode:
            return
        if term.rule or len(term.rule.strip()) == 0:
            # 没有定义规则，就只对产品词匹配
            self.postFixExp.append(term.name)
        else:
            #import pdb;pdb.set_trace()
            rule = term.rule.replace(' ', '')       # 先把所有空格去掉
            comma_chn = ("，").decode('utf-8')      # 中文"，"处理
            rule = rule.replace(comma_chn, ",")     # 避免人为失误，把，替换成,
            opStack = []                            # 定义了规则，把规则（中缀表达式）转换成后缀表达式
            try:
                bytes = rule.encode('utf-8')
                tmp = 0
                for i in range(0, len(bytes)):
                    if bytes[i] == ',' or bytes[i] == '|':
                        op = self.newStrFromBytes(bytes, i, i + 1)
                        if tmp < i:
                            # 遇到了操作符,把前面的非操作符串提取
                            str = self.newStrFromBytes(bytes, tmp, i)
                            self.postFixExp.append(str)
                        tmp = i + 1
                        if len(opStack) > 0:
                            while len(opStack) > 0 and opStack[-1] != '(':
                                self.postFixExp.append(opStack.pop())
                        opStack.append(op)
                    elif bytes[i] == '(':
                        # '('前面必然是操作符，本身是开头,所以无需提取非操作符的字符串
                        tmp = i + 1
                        # '('有最高的优先级别，直接入栈
                        opStack.append('(')
                    elif bytes[i] == ')':
                        # 把前面的非操作符串提取
                        if tmp < i:
                            str = self.newStrFromBytes(bytes, tmp, i)
                            self.postFixExp.append(str)
                        tmp = i + 1
                        # 把栈里面的操作符全部弹出，直到遇到(
                        while len(opStack) > 0 and opStack[-1] != '(':
                            self.postFixExp.append(opStack.pop())
                        if len(opStack) > 0 and opStack[-1] == "(":
                            opStack.pop()
                if tmp != len(bytes):
                    self.postFixExp.append(self.newStrFromBytes(bytes, tmp, len(bytes)))
                while len(opStack) > 0:
                    self.postFixExp.append(opStack.pop())

            except:
                logger.error("term %s buildRule failed: %s", term.cid, traceback.format_exc())

    def matchTerm(self, term_str, item):
        matchTerm = ""
        matched = True
        isNot = None
        if term_str.find('!') == 0:
            matchTerm = term_str[1:]
            isNot = True
        else:
            matchTerm = term_str
            isNot = False

        matchAt = ""
        parts = matchTerm.split('@')
        if len(parts) >= 2:
            matchTerm = parts[0]
            at = parts[1]
            if at.lower() == 'title':
                matchAt = item.title
            elif at.lower() == 'category':
                matchAt = item.category
            else:
                matchAt = item.title + ' ' + item.category
        else:
            matchAt = item.title

        if not matchAt:
            return False
        else:
            matchAt = matchAt.lower().encode("utf-8")

        if matchAt.find(matchTerm.lower()) > -1:
            matched = True
        else:
            matched = False
        
        if isNot:
            matched = not matched
        return matched

    def newStrFromBytes(self, bytes, start, end):
        rtn = ''
        try:
            val = bytes[start:end]
            rtn = val.decode('utf-8')
        except:
            logger.error("%s[%s, %s] newStrFromBytes failed: %s", bytes, start, end, traceback.format_exc())
        return rtn

    def match(self, item):
        result = False
        stack = []
        for i in range(0, len(self.postFixExp)):
            pfx = self.postFixExp[i].encode("utf-8")
            if pfx == ',':
                b1 = stack.pop()
                b2 = stack.pop()
                result = b1 and b2
                stack.append(result)
            elif pfx == '|':
                b1 = stack.pop()
                b2 = stack.pop()
                result = b1 or b2
                stack.append(result)
            else:
                result = self.matchTerm(pfx, item)
                stack.append(result)
        return result


if __name__ == "__main__":
    log_init("CrawlLogger", "sqlalchemy.*")
    
    db = get_db_engine()
    item = TaobaoItem(1624, 2000278, '18381030933')
    item.category = '蕾丝衫/雪纺衫,女装/女士精品,'
    item.title = '七格格 OTHERMIX 夏装新款 个性印花无袖雪纺衫 女中长款3MR2024P'
    term_factory = TermFactory(db)
    print item.matchTaobaoTerms(term_factory, "2,81,296")

