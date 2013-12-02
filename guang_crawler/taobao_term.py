#coding:utf8

import gflags
import logging
import traceback

from pygaga.helpers.logger import log_init
from pygaga.helpers.dbutils import get_db_engine
from guang_crawler.taobao_item import TaobaoItem

FLAGS = gflags.FLAGS
logger = logging.getLogger('CrawlLogger')

class TermFactory:
    def __init__(self, db):
        self.db = db
        self.all_terms = {}     # {cid:term}

        # init term data
        self.loadTerms()


    def loadTerms(self):
        terms_list = list(self.db.execute("select cid, parent_cid, is_parent, name, rule, is_other from term where cid in (580,579,581,577,578,575,576,573)"))
        for term_obj in terms_list:
            term = Term(term_obj[0], term_obj[1], term_obj[2], term_obj[3], term_obj[4], term_obj[5])
            self.all_terms[term.cid] = term

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
        if not term.rule or len(term.rule.strip()) == 0:
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
            matchAt = matchAt.lower()

        try:
            if matchAt.find(matchTerm.lower()) > -1:
                matched = True
            else:
                matched = False
        except:
            matched = False
            logger.error("UnicodeDecodeError:matchAt=%s-%s, matchTerm=%s-%s, matchTerm.lower()=%s-%s", matchAt,
                         type(matchAt), matchTerm, type(matchTerm), matchTerm.lower(), type(matchTerm.lower()))
        
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


