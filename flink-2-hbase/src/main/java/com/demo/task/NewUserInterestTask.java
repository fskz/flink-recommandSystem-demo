package com.demo.task;

import com.demo.domain.LogEntity;
import com.demo.map.UserHistoryWithInterestMapFunction;
import com.demo.map.UserInterestMapFunction;
import com.demo.util.LogToEntity;
import com.demo.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class NewUserInterestTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("interest");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
//        dataStream.print("kafkaSource>>>>>>>>");
        SingleOutputStreamOperator<Tuple2<LogEntity, Integer>> dsMap = dataStream
                .map(LogToEntity::getLog)
//                .keyBy(logEntity -> Tuple2.of(logEntity.getUserId(), logEntity.getProductId()))
                .keyBy(new KeySelector<LogEntity, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(LogEntity logEntity) throws Exception {
                        return Tuple2.of(logEntity.getUserId(), logEntity.getProductId());
                    }
                })
//                .map(new UserHistoryWithInterestMapFunction());
                .map(new UserInterestMapFunction());
//        dataStream.map(new GetLogFunction()).keyBy("userId").map(new UserHistoryWithInterestMapFunction());
        dsMap.print("dsMap>>>>>>>>>>>>>");
        env.execute("User Product History");
    }
}
