# coding: utf-8
import sys



class TermRule:

    def __init__(self,term):
        self.buildRule(term)

    def match(self,para):
        self.v = []
        self.valu()

    def buildRule(self,term):
        self.postFixExp = []
        if type(term['rule']) != unicode or type(term['name']) != unicode:
            return
        if term['rule'] == None or len(term['rule'].strip()) == 0:
            self.postFixExp.append(term['name'])
        else:
            #import pdb;pdb.set_trace()
            rule = term['rule'].replace(' ','')
            comma_chn = ("，").decode('utf-8')
            rule = rule.replace(comma_chn, ",")
            opStack = []
            try:
                bytes = rule.encode('utf-8')
                tmp = 0
                for i in range(0,len(bytes)):
                    if bytes[i] == ',' or bytes[i] == '|':
                        op = self.newStrFromBytes( bytes,i,i+1)
                        if tmp < i:
                            str = self.newStrFromBytes(bytes,tmp,i)
                            self.postFixExp.append(str)
                        tmp = i+1
                        if len(opStack) > 0:
                            while len(opStack)>0 and opStack[len(opStack)-1] != '(':
                                self.postFixExp.append(opStack.pop())
                        opStack.append(op)
                    elif bytes[i] == '(':
                        tmp = i+1
                        opStack.append('(')
                    elif bytes[i] == ')':
                        if tmp < i :
                            str = self.newStrFromBytes(bytes,tmp,i)
                            self.postFixExp.append(str)
                        tmp = i+1
                        while len(opStack) >0 and opStack[len(opStack)-1] != '(':
                            self.postFixExp.append(opStack.pop())
                        if opStack[len(opStack)-1] == "(":
                            opStack.pop()
                if tmp != len(bytes):
                    self.postFixExp.append(self.newStrFromBytes(bytes,tmp,len(bytes)))
                while len(opStack) > 0:
                    self.postFixExp.append(opStack.pop())

            except Exception ,e:
                print e

    def matchTerm(self,term,item):
        matchTerm = None
        matched = True
        isNot = None
        if term.find('!') == 0:
            matchTerm = term[1:]
            isNot = True
        else :
            matchTerm = term
            isNot = False

        matchAt = None
        parts = matchTerm.split('@')
        if len(parts) >=2:
            matchTerm = parts[0]
            at = parts[1]
            if at.lower() == 'title':
                matchAt = item['title']
            elif at.lower() == 'category':
                matchAt = item['category']
            else :
                matchAt = item['title']+' '+item['category']
        else:
            matchAt = item['title']
        if matchAt == None:
            return False
        else :
            matchAt = matchAt.lower()
        if matchAt.find(matchTerm.lower()) > -1:
            matched = True
        else:
            matched = False
        if isNot:
            matched = not matched
        return matched

    def newStrFromBytes(self,bytes,start,end):
        rtn = ''
        try:
            val = bytes[start:end]
            rtn = val.decode('utf-8')
        except BaseException ,e:
            print e
        return rtn

    def match(self,item):
        result = False
        stack = []
        for i in range(0,len(self.postFixExp)):
            str = self.postFixExp[i]
            if str == ',':
                b1 = stack.pop()
                b2 = stack.pop()
                result = b1 and b2
                stack.append(result)
            elif str == '|':
                b1 = stack.pop()
                b2 = stack.pop()
                result = b1 or b2
                stack.append(result)
            else:
                result = self.matchTerm(str,item)
                stack.append(result)
        return result

if __name__ == "__main__":
    term = {'rule':'(牛仔外套|牛仔衣|牛仔服),(女@category|女@title)，!裤,!裙,!衬衫,!衬衣'.decode('utf-8') ,'name':'牛仔外套'.decode('utf-8')}
    item = {'title':'韩国秋冬新款韩版甜美风名媛休闲拉链袖口长袖蝙蝠款卫衣连衣裙女'.decode('utf-8'),'category':'连衣裙,女装/女士精品,'.decode('utf-8')}
    tr = TermRule(term)
    print tr.match(item)
