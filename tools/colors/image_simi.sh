#!/bin/sh

set -e
set -x

export JAVA_HOME=/usr/lib/jvm/java-6-sun
export PIG_CLASSPATH=/usr/share/java/jython-pig-2.5.0.jar
export HADOOP_CLASSPATH=`ls /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.2*.jar -t|head -n 1`

input_path=/user/chris/image_digest
hdp_path=/tmp/chris/image_simi

/home/ldap/maoxing.xu/soft/pig-0.10.1/bin/pig \
    -Dmapred.create.symlink=yes \
    -param INPUT="$input_path/part*" \
    -param INPUT1="$input_path/part*" \
    -param HADOOP_OUTPUT=$hdp_path \
    ./image_simi.pig

# local test
#export PIG_CLASSPATH=/usr/share/java/jython-pig-2.5.0.jar
#pig -x local -param INPUT='test.txt' -param INPUT1='test1.txt' -param HADOOP_OUTPUT='result.txt' image_simi.pig

