package com.htdata.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

public class ReadFromKafka {
    public static void main(String[] args) throws Exception {
        // 构建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        //这里是由一个kafka
        properties.setProperty("bootstrap.servers", "你的ip:9092");
        properties.setProperty("group.id", "flink_consumer");
        //第一个参数是topic的名称
        DataStream<String> stream=env.addSource(new FlinkKafkaConsumer<String>("flink", new SimpleStringSchema(), properties));

        DataStream<Tuple3<Integer, String, Integer>> sourceStreamTra = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return StringUtils.isNotBlank(value);
            }
        }).map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple3<Integer, String, Integer> map(String value)
                    throws Exception {
                String[] args = value.split(",");
                return new Tuple3<Integer, String, Integer>(Integer
                        .valueOf(args[0]), args[1],Integer
                        .valueOf(args[2]));
            }
        });

        sourceStreamTra.addSink(new MysqlSink());
        env.execute("data to mysql start");


    }
}