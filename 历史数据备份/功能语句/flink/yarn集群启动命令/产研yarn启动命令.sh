#!/usr/bin/env bash

set -x

. /etc/profile.d/jdk.sh
. /etc/profile.d/hadoop_env.sh

PATH=$PATH:${HADOOP_HOME}/bin:${FLINK_HOME}/bin
BASE_DIR=$(cd $(dirname $0)/..; pwd)
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}:${CONF}:${LIB}*
export FLOW_APPLICATION_DIR=${BASE_DIR}

export HADOOP_CLASSPATH=`hadoop classpath`

/data1/apps/lib/flink/bin/flink run-application -t yarn-application \
    -Dyarn.ship-files="/data1/apps/cdp/apps/gio-flow/lib" \
    -Dyarn.application.name="KafkaSplitFlowApp_newkfk" \
    -Djobmanager.memory.process.size=1024MB \
    -Dtaskmanager.memory.process.size=5120MB \
    -Dtaskmanager.memory.managed.fraction=0.1 \
    -Drest.flamegraph.enabled=true \
    -Dexecution.buffer-timeout=1 \
    -Dtaskmanager.numberOfTaskSlots=12 \
    -p 50 -ynm KafkaSplitFlowApp-ynm \
    -yqu root.app \
    -Dyarn.flink-dist-jar=/data1/apps/lib/flink/lib/flink-dist_2.11-1.14.2.jar \
    -c com.gio.app.KafkaSplitFlowApp_newkfk \
    /data1/apps/ys/flink-ys-stream-1.0.110.jar

[app@application-2 ys]$ cat /etc/profile.d/jdk.sh
export JAVA_HOME="/data1/apps/lib/jdk"
export PATH=$JAVA_HOME/bin:$PATH
export TZ='UTC'
export JAVA_TOOL_OPTIONS='-Duser.timezone=UTC -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 -Dlog4j2.formatMsgNoLookups=true'
export FORMAT_MESSAGES_PATTERN_DISABLE_LOOKUPS=true


[app@application-2 ys]$ cat /etc/profile.d/hadoop_env.sh
export HADOOP_HOME=/data1/apps/lib/hadoop
export PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH
export HADOOP_CONF_DIR=/data1/apps/lib/hadoop/etc/hadoop


[app@application-2 ys]$ hadoop classpath
/data1/apps/lib/hadoop/etc/hadoop:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/common/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/common/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/hdfs:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/hdfs/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/hdfs/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/yarn:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/yarn/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/yarn/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/mapreduce/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/mapreduce/*:
/data1/apps/lib/hadoop/contrib/capacity-scheduler/*.jar



app@yarnflink-1 环境配置
[app@yarnflink-1 bin]$ echo $PATH
/home/app/.local/bin:/home/app/bin:
/data1/apps/lib/jdk/bin:
/data1/apps/lib/hadoop/sbin:
/data1/apps/lib/hadoop/bin:
/usr/local/bin:/usr/bin:
/usr/local/sbin:/usr/sbin


[app@yarnflink-1 bin]$ hadoop classpath
/data1/apps/lib/hadoop/etc/hadoop:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/common/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/common/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/hdfs:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/hdfs/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/hdfs/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/yarn:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/yarn/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/yarn/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/mapreduce/lib/*:
/data1/apps/lib/hadoop-2.10.2/share/hadoop/mapreduce/*:
/data1/apps/lib/hadoop/contrib/capacity-scheduler/*.jar



启动flink on yarn 
1、配置环境变量
$ sudo vim /etc/profile.d/my_env.sh
HADOOP_HOME=/opt/module/hadoop-2.7.5
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_CLASSPATH=`hadoop classpath`






#!/usr/bin/env bash

set -x

. /etc/profile.d/jdk.sh
. /etc/profile.d/hadoop_env.sh

PATH=$PATH:${HADOOP_HOME}/bin:${FLINK_HOME}/bin
BASE_DIR=$(cd $(dirname $0)/..; pwd)
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}:${CONF}:${LIB}*
export FLOW_APPLICATION_DIR=${BASE_DIR}

export HADOOP_CLASSPATH=`hadoop classpath`

/data1/apps/lib/flink/bin/flink run-application -t yarn-application \
    -Dyarn.ship-files="/data1/apps/cdp/apps/gio-flow/lib" \
    -Dyarn.application.name="test" \
    -Djobmanager.memory.process.size=1024MB \
    -Dtaskmanager.memory.process.size=5120MB \
    -Dtaskmanager.memory.managed.fraction=0.1 \
    -Drest.flamegraph.enabled=true \
    -Dexecution.buffer-timeout=1 \
    -Dtaskmanager.numberOfTaskSlots=6 \
    -yt /data1/apps/ys/clickhouse-jdbc-0.2.4.jar \
    -p 5 -ynm test \
    -yqu root.app \
    -Dyarn.flink-dist-jar=/data1/apps/lib/flink/lib/flink-dist_2.11-1.14.2.jar \
    -c com.atic.cm.stream.test \
    /data1/apps/ys/log-stream-1.0.1.jar

    flink run -d -p10  -c com.bigdata.flink.KafkaToRightSmarketGio /home/app/cm-log-stream/gio-flink-new.jar --dev true