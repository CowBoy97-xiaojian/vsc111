package com.doit.day07;


import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Date: 2023/7/1
 * @Author: Hang.Nian.YY
 * @WX: 17710299606
 * @Tips: 学大数据 ,到多易教育
 * @DOC: https://blog.csdn.net/qq_37933018?spm=1000.2115.3001.5343
 * @Description:
 */
public class E06BroadCast {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8888);
        StreamExecutionEnvironment see = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        see.setParallelism(1);
        // 会员等级信息 [维度表 / 规则]   广播流
        DataStreamSource<Tuple2<Integer, String>> ds1 = see.fromElements(
                Tuple2.of(1, "普通会员"),
                Tuple2.of(2, "白银会员"),
                Tuple2.of(3, "铂金会员"),
                Tuple2.of(4, "钻石会员")
        );
        数据在状态后端中的存储结构 , 广播流只支持Map类型
        MapStateDescriptor<Integer, Tuple2<Integer, String>> mapStateDescriptor = new MapStateDescriptor<>("level", TypeInformation.of(new TypeHint<Integer>() {
        }), TypeInformation.of(new TypeHint<Tuple2<Integer, String>>() {
        }));
        //-------------------广播流
        // 将个规则 流转换成广播流
        BroadcastStream<Tuple2<Integer, String>> broadcastStream = ds1.broadcast(mapStateDescriptor);

       //------------------主流
        //获取主流数据   <uid,event,levelid>
        DataStreamSource<String> ds2 = see.socketTextStream("doitedu01", 8899);
        SingleOutputStreamOperator<Tuple3<String, String, String>> mainData = ds2.map(line -> {
            String[] arr = line.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
        }));

        //
        mainData.connect(broadcastStream).process(new BroadcastProcessFunction<Tuple3<String, String, String>, Tuple2<Integer, String>, Tuple4<String, String, String, String>>() {

            BroadcastState<Integer, Tuple2<Integer, String>> broadcastState ;
           // 3 处理每个主流的数据
            // 获取状态中的  会员等级信息  关联上  输出
            @Override
            public void processElement(Tuple3<String, String, String> value, BroadcastProcessFunction<Tuple3<String, String, String>, Tuple2<Integer, String>, Tuple4<String, String, String, String>>.ReadOnlyContext ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                Tuple2<Integer, String> tp = broadcastState.get(Integer.parseInt(value.f2));
                if(tp!=null){
                    out.collect(Tuple4.of(value.f0 , value.f1,value.f2 , tp.f1));
                }else{
                    out.collect(Tuple4.of(value.f0 , value.f1,value.f2 , null));
                }
            }
           // 1 处理广播流
            // 2 获取广播流中的所有数据 将数据存储在状态中
            @Override
            public void processBroadcastElement(Tuple2<Integer, String> value, BroadcastProcessFunction<Tuple3<String, String, String>, Tuple2<Integer, String>, Tuple4<String, String, String, String>>.Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put(value.f0 , value);
            }
        }).print() ;

        see.execute() ;



    }
}