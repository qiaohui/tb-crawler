register 'crosscorr.py' using jython as simiudf;

data = load '$INPUT' using PigStorage(',');
data1 = load '$INPUT1' using PigStorage(',');

g_data = group data by $1;
g_data1 = group data1 by $1;

conv_data = foreach g_data generate $0, simiudf.to_bag(*);
conv_data1 = foreach g_data1 generate $0, simiudf.to_bag(*);

cross_data = cross conv_data, conv_data1 parallel 10;
cross_data1 = filter cross_data by $0 > $2 parallel 20;

--dump cross_data1;
--describe cross_data1;

result = foreach cross_data1 generate $0, $2, simiudf.crosscorr2($1, $3) parallel 100;
filtered_result = filter result by $2 > 0.7;

store filtered_result into '$HADOOP_OUTPUT';
--dump result;

