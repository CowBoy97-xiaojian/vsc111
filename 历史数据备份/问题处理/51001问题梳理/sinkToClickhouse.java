package com.doit.day04;


import com.doit.beans.LogBean;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Date: 2023/6/27
 * @Author: Hang.Nian.YY
 * @WX: 17710299606
 * @Tips: 学大数据 ,到多易教育
 * @DOC: https://blog.csdn.net/qq_37933018?spm=1000.2115.3001.5343
 * @Description: 接收网络数据
 * 处理
 * sink到mysql
 * <p>
 * sink  不保证精确处理一次
 * exactlySink 保证精确处理一次
 */
public class E01JdbcSink02 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8888);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        see.setParallelism(1); // 设置全局并行度

        see.enableCheckpointing(2000) ;

        DataStreamSource<String> data = see.socketTextStream("doitedu01", 8899);

        SingleOutputStreamOperator<LogBean> logBeans = data.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String value) throws Exception {
                String[] arr = value.split(",");
                return LogBean.of(Long.parseLong(arr[0]), arr[1], arr[2], Long.parseLong(arr[3]));
            }
        });
        //sink(String sql, JdbcStatementBuilder<T> statementBuilder, JdbcExecutionOptions executionOptions, JdbcConnectionOptions connectionOptions) {
        //主键相同数据更新
        // 参数一
        String sql = "insert into  tb_log values(?,?,?,?) on duplicate key update  session_id= ? , event_name = ? , ts = ?";
     // 参数二 预编译sql
        JdbcStatementBuilder<LogBean> jdbcStatementBuilder = new JdbcStatementBuilder<LogBean>() {
            @Override
            public void accept(PreparedStatement preparedStatement, LogBean logBean) throws SQLException {
                // 预编译sql
                preparedStatement.setLong(1, logBean.getGuid());
                preparedStatement.setString(2, logBean.getSessionId());
                preparedStatement.setString(3, logBean.getEventName());
                preparedStatement.setLong(4, logBean.getTs());

                preparedStatement.setString(5, logBean.getSessionId());
                preparedStatement.setString(6, logBean.getEventName());
                preparedStatement.setLong(7, logBean.getTs());
            }
        };
        /**
         * 参数三    执行相关参数
         */
        JdbcExecutionOptions jdbcExecutionOptions = JdbcExecutionOptions.builder()
                // 必须设置成 0
                .withMaxRetries(0)  // 连接服务器重试次数
                .withBatchSize(2)   // 批执行
                .withBatchIntervalMs(5000)  // 间隔触发写入
                .build();
        /**
         * 参数四
         * 保证sink端  数据 Exactly once  要有事务加持
         */
        JdbcExactlyOnceOptions exactlyOnceOptions = JdbcExactlyOnceOptions
                .builder()
                // .withAllowOutOfOrderCommits(true)
                // 有些数据库支持  一个连接中多个事务   mysql只支持一个链接中一个事务
                //  mysql只支持一个链接中一个事务    mysql必须写true
                .withTransactionPerConnection(true)
              //  .withMaxCommitAttempts(10)  // 事务最大提交次数
                .build();
        /**
         * 参数五  连接数据库相关的参数
         */
        SerializableSupplier<XADataSource> serializableSupplier = new SerializableSupplier<XADataSource>() {
            @Override
            public XADataSource get() {
                MysqlXADataSource mysqlXADataSource = new MysqlXADataSource();
                mysqlXADataSource.setUrl("jdbc:mysql://localhost:3306/doe");
                mysqlXADataSource.setUser("root");
                mysqlXADataSource.setPassword("root");
                return mysqlXADataSource;
            }
        };
        /**
         * 参数一 执行的sql
         * 参数二  Statement
         * 参数三  执行相关参数
         * 参数四  和精确处理一次相关的参数
         * 参数五  和数据库连接相关
         */
        SinkFunction<LogBean> logBeanSinkFunction = JdbcSink.exactlyOnceSink(sql, jdbcStatementBuilder, jdbcExecutionOptions, exactlyOnceOptions, serializableSupplier);
        logBeans.addSink(logBeanSinkFunction) ;
        see.execute();
    }
}