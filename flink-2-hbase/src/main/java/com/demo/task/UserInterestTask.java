package com.demo.task;

import com.demo.domain.LogEntity;
import com.demo.map.GetLogFunction;
import com.demo.map.UserHistoryWithInterestMapFunction;
import com.demo.util.LogToEntity;
import com.demo.util.Property;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class UserInterestTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("interest");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties));
        dataStream.print("kafkaSource>>>>>>>>");
        dataStream
                .map(LogToEntity::getLog)
                .keyBy(logEntity -> Tuple2.of(logEntity.getUserId(), logEntity.getProductId()))
                .map(new UserHistoryWithInterestMapFunction());
//                .map(new UserHistoryWithInterestMapFunction());
//        dataStream.map(new GetLogFunction()).keyBy("userId").map(new UserHistoryWithInterestMapFunction());

        env.execute("User Product History");
    }
}
