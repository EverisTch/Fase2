#!/bin/bash
hdfs dfs -put -f *.* /user/srv_ingesta/oozie/urm/mobile_line
hdfs dfs -ls /user/srv_ingesta/oozie/urm/mobile_line
hdfs dfs -chmod 755 /user/srv_ingesta/oozie/urm/mobile_line/run_processor.sh
hdfs dfs -ls /user/srv_ingesta/oozie/urm/mobile_line
