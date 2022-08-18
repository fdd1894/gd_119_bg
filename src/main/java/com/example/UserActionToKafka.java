package com.example;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class UserActionToKafka {
    public static void main(String[] args) throws Exception {
        writeUserActionToKafka("user");
        //搭建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //降低并行度便于测试
        env.setParallelism(1);
        //读取kafka的user topic 读数据
        DataStream<String> kafkaDS = readKafka(env, "user");
        //处理数据
        SingleOutputStreamOperator<userAction> mapDS = kafkaDS
                //将kafka的jason数据转成pojo对象
                .map(new MapFunction<String, userAction>() {
                    @Override
                    public userAction map(String value) throws Exception {
                        return JSON.parseObject(value, userAction.class);
                    }
                })
                //过滤数据，拿到pv数据
                .filter(x -> "pv".equals(x.behavior));

        mapDS.print();
        //设置水位线

        SingleOutputStreamOperator<userAction> userActionWatermark = mapDS.assignTimestampsAndWatermarks(
                //三种水位线：
                //forBoundedOutOfOrderness 有界无序 在3秒内
                WatermarkStrategy.<userAction>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        //按照事件事件为水位线
                        .withTimestampAssigner((event, timestamp) -> event.eventTimeAction)
        );


        KeyedStream ksm = userActionWatermark.keyBy(t -> t.itemId);
        WindowedStream wst= ksm.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<FactUser> aggregate = wst.aggregate(new userActionAggregate(),new userProcessWindowFunction());
/*        SingleOutputStreamOperator<FactUser> aggregate = userActionWatermark
                //按照需求分组
                .keyBy(t -> t.itemId)
                //设置滚动窗口或滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //聚合（增量聚合，全量聚合）
                .aggregate(new userActionAggregate(), new userProcessWindowFunction());*/

//        aggregate.print();
        System.out.println("======================================");

        SingleOutputStreamOperator<FactUser> processDS = aggregate
                //分组聚合00000000
                .keyBy(t -> t.reportTime)
                //全量聚合
                .process(new TopN(5));
        processDS.print();
        // processDS.addSink(new MySQLSink());
        env.execute();

    }

    // 包装一个写入kafka的方法
    public static void writeUserActionToKafka(String topic) throws Exception {
        // kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:2181");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        Random random = new Random();
        String[] behaviors = {"pv", "buy", "cart", "fav"};
        while (true) {
            Thread.sleep(1);
            //随机用户
            Long userId = Math.round(random.nextDouble() * 10);
            //随机商品
            Long itemid = Math.round(random.nextDouble() * 10);
            //随机分类
            Long categoryId = Math.round(random.nextDouble() * 1000);
            //随机行为类型
            int index = random.nextInt(behaviors.length);
            String behavior = behaviors[index];
            Long currentTime = System.currentTimeMillis() ;

            userAction user = new userAction();
            user.setUserId(userId);
            user.setItemId(itemid);
            user.setCategoryId(categoryId);
            user.setBehavior(behavior);
            user.setEventTimeAction(currentTime);
            String u = JSON.toJSONString(user);
            kafkaProducer.send(new ProducerRecord<String, String>(topic,u));
            System.out.println(u);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class userAction {
        private Long userId;
        private Long itemId;
        private Long categoryId;
        private String behavior;
        private Long eventTimeAction;

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getItemId() {
            return itemId;
        }

        public void setItemId(Long itemId) {
            this.itemId = itemId;
        }

        public Long getCategoryId() {
            return categoryId;
        }

        public void setCategoryId(Long categoryId) {
            this.categoryId = categoryId;
        }

        public String getBehavior() {
            return behavior;
        }

        public void setBehavior(String behavior) {
            this.behavior = behavior;
        }

        public Long getEventTimeAction() {
            return eventTimeAction;
        }

        public void setEventTimeAction(Long eventTimeAction) {
            this.eventTimeAction = eventTimeAction;
        }

        @Override
        public String toString() {
            return "userAction{" +
                    "userId=" + userId +
                    ", itemId=" + itemId +
                    ", categoryId=" + categoryId +
                    ", behavior='" + behavior + '\'' +
                    ", eventTimeAction=" + new Timestamp(eventTimeAction) +
                    '}';
        }

    }


    private static class FactUser {
        private Long itemId;
        private Long aggCount;
        private Long reportTime;

        public Long getItemId() {
            return itemId;
        }

        public void setItemId(Long itemId) {
            this.itemId = itemId;
        }

        public Long getAggCount() {
            return aggCount;
        }

        public void setAggCount(Long aggCount) {
            this.aggCount = aggCount;
        }

        public Long getReportTime() {
            return reportTime;
        }

        public void setReportTime(Long reportTime) {
            this.reportTime = reportTime;
        }

        @Override
        public String toString() {
            return "FactUser{" +
                    "itemId=" + itemId +
                    ", aggCount=" + aggCount +
                    ", reportTime=" + new Timestamp(reportTime) +
                    '}';
        }
    }

    /**
     * 从kafka读取数据
     * @param env
     * @param topic
     * @return
     */
    private static DataStream<String> readKafka(StreamExecutionEnvironment env, String topic) {
        Properties props = new Properties();
        //集群地址
        props.setProperty("bootstrap.servers", "192.168.52.100:9092");
        //消费者组id
        props.setProperty("group.id", "flink");
        //从最新的地方开始读取
        props.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);
        return kafkaDS;
    }

    /**
     * 增量数据处理
     */
    private static class userActionAggregate implements AggregateFunction<userAction, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(userAction value, Long accumulator) {
            return accumulator += 1;
        }

        @Override
        public Long getResult(Long
                                      accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 全量信息处理
     */
    private static class userProcessWindowFunction extends ProcessWindowFunction<Long, FactUser, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<Long> elements, Collector<FactUser> out) throws Exception {
            Long next = elements.iterator().next();
            FactUser factUser = new FactUser();
            factUser.setItemId(key);
            factUser.setAggCount(next);
            factUser.setReportTime(context.window().getEnd());
            out.collect(factUser);
        }
    }

    /**
     * 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
     * K , I , O
     */
    public static class TopN extends KeyedProcessFunction<Long, FactUser, FactUser> {
        private final int n;

        public TopN(int n) {
            this.n = n;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private transient ListState<FactUser> itemState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<FactUser> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    FactUser.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(FactUser itemViewCount, Context context, Collector<FactUser> collector) throws Exception {
            // 每条数据都保存到状态中
            this.itemState.add(itemViewCount);
            // 注册 windowEnd + 1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(itemViewCount.getReportTime() + 1);
        }

        // -------------------------------------------
        // 定时器的代码实现
        // -------------------------------------------
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<FactUser> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 获取收到的所有商品点击量
            List<FactUser> allItems = new ArrayList<>();
            for (FactUser item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort((o1, o2) -> Long.compare(o2.aggCount, o1.aggCount));

            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
            if (allItems.size() < n) {
                for (int i = 0; i < allItems.size(); i++) {
                    FactUser currentItem = allItems.get(i);
                    result.append("No").append(i).append(":")
                            .append("  商品ID=").append(currentItem.getItemId())
                            .append("  浏览量=").append(currentItem.getAggCount())
                            .append("\n");
                    out.collect(currentItem);
                }
                result.append("====================================\n");

            } else {
                for (int i = 0; i < n; i++) {
                    FactUser currentItem = allItems.get(i);
                    result.append("No").append(i).append(":")
                            .append("  商品ID=").append(currentItem.getItemId())
                            .append("  浏览量=").append(currentItem.getAggCount())
                            .append("\n");
                    out.collect(currentItem);
                }
                result.append("====================================\n");

            }
        }
    }

    /**
     * 数据写入mysql
     */
    public static class MySQLSink extends RichSinkFunction<FactUser> {
        private Connection conn = null;
        private PreparedStatement ps = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/exam", "root", "123456");
            String sql = "INSERT INTO `hot_goods_report` (`id`,`itemId`, `aggCount`, `reportTime`) VALUES (null,?, ?, ?);";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void invoke(FactUser value, Context context) throws Exception {
            ps.setLong(1, value.itemId);
            ps.setLong(2, value.aggCount);
            ps.setLong(3, value.reportTime);
            ps.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
        }
    }

}
