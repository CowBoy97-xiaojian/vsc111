
. /etc/profile.d/jdk.sh
. /etc/profile.d/hadoop_env.sh

#在 Flink 集群环境中，经常需要运行在 JDK 和 Hadoop 环境下。为了确保在 Flink 中可以正确地使用这些环境，需要在启动 Flink 之前加载相应的环境变量和配置。

#因此，flink 常常需要执行如下两个脚本来获得正确的 JAVA 和 Hadoop 环境：

#/etc/profile.d/jdk.sh
这个脚本会设置 JAVA_HOME 等系统环境变量，以便在 Flink 中正确地使用 Java 运行时环境。

#/etc/profile.d/hadoop_env.sh
#这个脚本会设置 HADOOP_HOME 和 HADOOP_CLASSPATH 等 Hadoop 相关的环境变量，以便在 Flink 中正确地使用 Hadoop 集群的数据存储和计算资源。

#加载这些脚本可以确保 Flink 集群在启动时具有正确的配置，并且所有 Flink 任务可以成功访问所需的 Java 运行时和 Hadoop 集群资源。