
import csv
from collections import namedtuple
from pygaga.model.feature import logit_stats_model, lg_stats_model, numberic2SignalFn

def train(features):
    #m = logit_stats_model()
    m = lg_stats_model()
    #import pdb; pdb.set_trace()
    m.train(features)
    m.dump_validation(features)

def dataType2Sigal(dt):
    price_seps = [50.0,200.0]
    volume_seps = [10,100,400]
    key_transform = {'price' : numberic2SignalFn(float, price_seps),
        'volume' : numberic2SignalFn(int, volume_seps)}
    signals = {}
    for key in key_transform:
        if hasattr(dt, key):
            v = getattr(dt, key)
            signal = "%s%s" % (key, key_transform[key](v))
            signals[signal] = 1
    return signals

def data2sigal():
    f = open("sample.txt")
    data = list(csv.reader(f))
    header = data[0]
    dataType = namedtuple("DataType", " ".join(header))
    features = []
    for l in data[1:]:
        dt = dataType(*l)
        signals = dataType2Sigal(dt)
        features.append((int(dt.y), signals))
    return features

if __name__ == "__main__":
    train(data2sigal())
