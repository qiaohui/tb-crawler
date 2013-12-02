from array import array
import math

#def outputSchema(arg):
#    def infunc(func):
#        def infunc2(*args, **kw):
#            return func(*args, **kw)
#        return infunc2
#    return infunc

@outputSchema("t:tuple()")
def to_tuple(groupkey, line):
    return tuple([int(x.tostring()) for x in line[0][2:]])

@outputSchema("f:double")
def crosscorr(x_coeffs, y_coeffs):
    """
    >>> crosscorr((123,123,3,4,4,5,6,7,8,11), (4,3,2,1,34,5,67,88,11,22))
    0.9257974624633789
    """
    size = len(y_coeffs)
    sumx = float(sum(x_coeffs))
    sumy = float(sum(y_coeffs))
    meanx = sumx / size
    meany = sumy / size
    max = 0
    r = array('f')
    for d in range(size):
        num = 0.0
        denx = deny = 0.0
        for i in range(size):
            #import pdb; pdb.set_trace()
            diffx = x_coeffs[i] - meanx
            diffy = y_coeffs[(size+i-d) % size] - meany
            num += diffx * diffy
            denx += math.pow(diffx, 2)
            deny += math.pow(diffy, 2)
            #print i,(size+i-d) % size,diffx,diffy,denx,deny
        denxy = math.sqrt(denx * deny)
        if denxy != 0.0:
            r.append(num / denxy)
        else:
            r.append(1.0)
        if r[d] > max:
            max = r[d]
    return max

@outputSchema("b:bag{}")
def to_bag(groupkey, line):
    coeffs = [int(x.tostring()) for x in line[0][2:]]
    total = float(sum(coeffs))
    mean = total / len(coeffs)
    diffa = tuple([i-mean for i in coeffs])
    sumdiffa = sum([math.pow(d, 2) for d in diffa])
    return [diffa, (sumdiffa,)]

@outputSchema("f:double")
def crosscorr2(x_bag, y_bag):
    xdiff, xsumdiff = x_bag
    ydiff, ysumdiff = y_bag
    size = len(xdiff)
    max = 0
    for d in range(size):
        num = 0.0
        for i in range(size):
            num += xdiff[i] * ydiff[(size+i-d) % size]
        if num > max:
            max = num
    sqrtv = math.sqrt(xsumdiff[0] * ysumdiff[0])
    if sqrtv > 0.000001:
        return max / sqrtv
    else:
        return 1.0

