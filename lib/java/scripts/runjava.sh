#!/bin/bash

base_dir="`pwd`"
lib_dir="${base_dir}/lib/"
local_lib_dir="${base_dir}/local_lib/"

CLASSPATH="${base_dir}/build/classes/:${base_dir}/build/tool/classes/:${base_dir}/build/test/:${base_dir}/build/tool/"

for f in ${lib_dir}*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done


for f in ${local_lib_dir}*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done


if [ "$1" = "" ]; then
    JAVACMD="echo \"USAGE: runjava.sh <-profile> [JAVA_CMD_PARAS]\""
else
    ## -Duser.dir=${lib_dir}
    JAVACMD="java -Xloggc:jvm.log -server -Xms500m -Xmn400m -Xmx500m -XX:MaxPermSize=212m -Dfile.encoding=UTF8 -classpath $CLASSPATH $*"
fi

## echo "------------------------------------------------------"
## echo $JAVACMD
## echo "------------------------------------------------------"

$JAVACMD
##  >> filelog.log  2>&1
