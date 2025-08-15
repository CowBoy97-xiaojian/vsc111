#!/usr/bin/env bash

set -x

. /etc/profile.d/jdk.sh
. /etc/profile.d/hadoop_env.sh

PATH=$PATH:${HADOOP_HOME}/bin:${FLINK_HOME}/bin
BASE_DIR=$(cd $(dirname $0)/..; pwd)
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}:${CONF}:${LIB}*
export FLOW_APPLICATION_DIR=${BASE_DIR}

export HADOOP_CLASSPATH=`hadoop classpath`

/data1/flink-1.16.3/bin/flink run-application -t yarn-application \
    -Dyarn.application.name="AppAutomatedQycsPg" \
    -Djobmanager.memory.process.size=1024MB \
    -Dtaskmanager.memory.process.size=5120MB \
    -Dtaskmanager.memory.managed.fraction=0.1 \
    -Drest.flamegraph.enabled=true \
    -Dexecution.buffer-timeout=1 \
    -Dtaskmanager.numberOfTaskSlots=12 \
    -yqu root.app \
    -p 10 -ynm AppAutomatedQycsPgStream \
    -c $1 $2


