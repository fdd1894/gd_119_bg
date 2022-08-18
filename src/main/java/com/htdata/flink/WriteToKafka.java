package com.htdata.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


import java.util.Properties;

/**
 * 写入kafka
 */
public class WriteToKafka {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "你的ip:9092");

        DataStream<String> stream = env.addSource(new SimpleStringGenerator());
        stream.addSink(new FlinkKafkaProducer<String>("flink", new SimpleStringSchema(), properties));  //配置topic
        env.execute();
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {

        long i = 0;
        boolean swith = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for(int k=0;k<5;k++) {
                ctx.collect("flink:" + k++);
            }
        }

        @Override
        public void cancel() {
            swith = false;
        }

    }



}