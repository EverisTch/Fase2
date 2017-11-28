#!/bin/bash
export OOZIE_URL=http://prd7432.bigdata.tchile.local:11000/oozie
oozie job -run -config job.properties -verbose -debug -auth kerberos


