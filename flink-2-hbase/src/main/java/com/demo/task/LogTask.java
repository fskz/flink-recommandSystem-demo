package com.demo.task;

import com.demo.domain.LogEntity;
import com.demo.sink.HBaseSink;
import com.demo.util.LogToEntity;
import com.demo.util.Property;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.hbase.client.Put;

import java.util.Properties;

/**
 * 日志 -> Hbase
 *
 * @author XINZE
 */
public class LogTask {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = Property.getKafkaProperties("log");
        DataStreamSource<String> dsKafka = env.addSource(
                new FlinkKafkaConsumer<String>("con", new SimpleStringSchema(), properties)
        );
        SingleOutputStreamOperator<LogEntity> ds = dsKafka.map(LogToEntity::getLog);
        ds.addSink(new HBaseSink<LogEntity>("con") {
            @Override
            public void invoke(LogEntity log, Context context) throws Exception {
                String rowKey = log.getUserId() + "_" + log.getProductId()+ "_"+ log.getTime();
                Put put = new Put(rowKey.getBytes());
                put.addColumn("log".getBytes(), "userid".getBytes(), String.valueOf(log.getUserId()).getBytes());
                put.addColumn("log".getBytes(), "productid".getBytes(), String.valueOf(log.getProductId()).getBytes());
                put.addColumn("log".getBytes(), "time".getBytes(), String.valueOf(log.getTime()).getBytes());
                put.addColumn("log".getBytes(), "action".getBytes(), String.valueOf(log.getAction()).getBytes());
                table.put(put);
            }
        });

        ds.print("log>>>>>>>>>>>>>");
        env.execute("Log message receive");
    }
}
