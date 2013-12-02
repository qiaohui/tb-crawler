import rbco.msexcel
import csv
import traceback
import gflags
import logging

gflags.DEFINE_string('input', '/Users/chris/workspace/taobao-crawler/guanglog/Taokedetail-2013-01-23.xls', 'input file name')
gflags.DEFINE_string('output', 'result.csv', 'output file name')

FLAGS = gflags.FLAGS

logger = logging.getLogger('GuangLogger')

def line2list(line):
    l = []
    for k in range(15):
        v = line.get(k, '')
        l.append(v)
    return l

def convert():
    excel_dict = rbco.msexcel.xls_to_excelerator_dict(FLAGS.input, encoding='utf8')
    records = rbco.msexcel.excelerator_dict_to_rows_and_columns(excel_dict)['Page1']
    csv_file = open(FLAGS.output, "w")
    csv_writer = csv.writer(csv_file)
    for r in records:
        l = map(str, line2list(records[r]))
        csv_writer.writerow(l)

if __name__ == "__main__":
    convert()

