#!/bin/bash
#archivo de prueba de ejecución

#echo "$1 $2 $3 $4" > /tmp/test1234.txt
#hdfs dfs -put /tmp/test1234.txt /tmp
#rm /tmp/test1234.txta

hdfs dfs -rm -r -f -skipTrash /user/srv_ingesta/mr_test_deleteme/output/bz2
hadoop jar $1 \
    -Dmapreduce.job.queue.name=hdpappHIGH \
    -Dmapred.output.compress=true \
    -Dmapred.compress.map.output=true \
    -Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
    -Dmapred.reduce.tasks=1 \
    -input /stage/urm/call_cdr/processing/*.txt \
    -output /user/srv_ingesta/mr_test_deleteme/output/bz2 \
    -mapper "/usr/bin/spicy \
    -columns=$4 \
    -salt="$3" \
    -sep="'|'" \
    -skip=false"
INPUT_FILE=/user/srv_ingesta/mr_test_deleteme/output/bz2/*.bz2
OUTPUT_FILE=/user/srv_ingesta/mr_test_deleteme/output/bz2/newFile.bz2
hdfs dfs -mv $INPUT_FILE $OUTPUT_FILE

