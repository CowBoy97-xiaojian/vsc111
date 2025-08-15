

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

    -Djobmanager.memory.process.size=1024MB \
    -Dtaskmanager.memory.process.size=5120MB \
    -Dtaskmanager.numberOfTaskSlots=12 \
    -p 5

    com.atic.cm.stream.test

    /data1/ys/log-stream-1.0.0.jar
/data1/flink/flink-1.16.3/bin/flink run-application -t yarn-application \
    -Dyarn.application.name="KafkaSplitFlowApp_newkfk" \
    -p 50 -ynm KafkaSplitFlowApp-ynm \
    -yqu root.default \
    -c com.atic.cm.stream.test \
    /data1/ys/log-stream-1.0.0.jar

/data1/flink/flink-1.16.3/lib

#!/usr/bin/env bash

set -x

. /etc/profile.d/jdk.sh
. /etc/profile.d/hadoop_env.sh

PATH=$PATH:${HADOOP_HOME}/bin:${FLINK_HOME}/bin
BASE_DIR=$(cd $(dirname $0)/..; pwd)
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}:${CONF}:${LIB}*
export FLOW_APPLICATION_DIR=${BASE_DIR}

export HADOOP_CLASSPATH=`hadoop classpath`

/data1/flink/flink-1.16.3/bin/flink run-application -t yarn-application \
    -Dyarn.application.name="test" \
    -Djobmanager.memory.process.size=1024MB \
    -Dtaskmanager.memory.process.size=5120MB \
    -Dtaskmanager.memory.managed.fraction=0.1 \
    -Drest.flamegraph.enabled=true \
    -Dexecution.buffer-timeout=1 \
    -Dtaskmanager.numberOfTaskSlots=12 \
    -p 50 -ynm KafkaSplitFlowApp-ynm \
    -yqu root.default \
    -Dyarn.flink-dist-jar=/data1/ys/flink-dist-1.16.3.jar \
    -c com.atic.cm.stream.test \
    /data1/ys/log-stream-1.0.0.jar

/data1/flink/flink-1.16.3/bin/flink run -d -p10  -c com.atic.cm.stream.test /data1/ys/log-stream-1.0.0.jar


    ------------------------------------------------------------
 The program finished with the following exception:

org.apache.flink.client.deployment.ClusterDeploymentException:Couldn't deploy Yarn Application Cluster
        at org.apache.flink.yarn.YarnClusterDescriptor.deployApplicationCluster(YarnClusterDescriptor.java:478)
        at org.apache.flink.client.deployment.application.cli.ApplicationClusterDeployer.run(ApplicationClusterDeployer.java:67)
        at org.apache.flink.client.cli.CliFrontend.runApplication(CliFrontend.java:207)
        at org.apache.flink.client.cli.CliFrontend.parseAndRun(CliFrontend.java:1090)
        at org.apache.flink.client.cli.CliFrontend.lambda$main$10(CliFrontend.java:1165)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1938)
        at org.apache.flink.runtime.security.contexts.HadoopSecurityContext.runSecured(HadoopSecurityContext.java:41)
        at org.apache.flink.client.cli.CliFrontend.main(CliFrontend.java:1165)
Caused by: org.apache.flink.yarn.YarnClusterDescriptor$YarnDeploymentException: The YARN application unexpectedly switched to state FAILED during deployment. 
Diagnostics from YARN: Application application_1697529567410_0005 failed 1 times (global limit =2; local limit is =1) due to AM Container for appattempt_1697529567410_0005_000001 exited with  exitCode: 239
Failing this attempt.Diagnostics: [2023-12-12 02:51:05.747]Exception from container-launch.
Container id: container_1697529567410_0005_01_000001
Exit code: 239

[2023-12-12 02:51:05.753]Container exited with a non-zero exit code 239. Error file: prelaunch.err.
Last 4096 bytes of prelaunch.err :

[2023-12-12 02:51:05.753]Container exited with a non-zero exit code 239. Error file: prelaunch.err.
Last 4096 bytes of prelaunch.err :

For more detailed output, check the application tracking page: http://yarnflink-2:8088/cluster/app/application_1697529567410_0005 Then click on links to logs of each attempt.
. Failing the application.
If log aggregation is enabled on your cluster, use this command to further investigate the issue:
yarn logs -applicationId application_1697529567410_0005
        at org.apache.flink.yarn.YarnClusterDescriptor.startAppMaster(YarnClusterDescriptor.java:1262)
        at org.apache.flink.yarn.YarnClusterDescriptor.deployInternal(YarnClusterDescriptor.java:623)
        at org.apache.flink.yarn.YarnClusterDescriptor.deployApplicationCluster(YarnClusterDescriptor.java:471)
        ... 9 more
2023-12-12 03:25:25,821 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Cancelling deployment from Deployment Failure Hook
2023-12-12 03:25:25,822 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Killing YARN application
2023-12-12 03:25:25,826 INFO  org.apache.hadoop.yarn.client.ConfiguredRMFailoverProxyProvider [] - Failing over to rm2
2023-12-12 03:25:25,831 INFO  org.apache.hadoop.yarn.client.api.impl.YarnClientImpl        [] - Killed application application_1697529567410_0005
2023-12-12 03:25:25,842 INFO  org.apache.flink.yarn.YarnClusterDescriptor                  [] - Deleting files in hdfs://growingFS/user/app/.flink/application_1697529567410_0005.
[app@yarnflink-1 ys]$ 


#!/usr/bin/env bash

set -x

. /etc/profile.d/jdk.sh
. /etc/profile.d/hadoop_env.sh

PATH=$PATH:${HADOOP_HOME}/bin:${FLINK_HOME}/bin
BASE_DIR=$(cd $(dirname $0)/..; pwd)
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}:${CONF}:${LIB}*
export FLOW_APPLICATION_DIR=${BASE_DIR}

export HADOOP_CLASSPATH=`hadoop classpath`

/data1/flink/flink-1.16.3/bin/flink run-application -t yarn-application \
            -c com.atic.cm.stream.test2 \
            /data1/ys/log-stream-1.0.1.jar


            /data1/flink/flink-1.16.3/bin/flink run  -m yarn-cluster -d \

com.atic.cm.stream.test2

com.atic.cm.stream.test2


/data1/flink/flink-1.16.3/bin/flink run-application -t yarn-application \
            -c org.myorg.quickstart.DataStreamJob \
            /data1/ys/original-quickstart-0.1.jar



com.atic.cm.stream.Test2
com.atic.cm.stream.Test