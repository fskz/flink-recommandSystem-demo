package com.demo.sink;

import com.demo.client.HbaseClient;
import com.demo.domain.LogEntity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseIncrementColumnSink extends RichSinkFunction<LogEntity> {
    private transient Connection connection;
    private String tableName;
    private String familyName;
    private String columnName;

    public HbaseIncrementColumnSink(String tableName, String familyName, String columnName) {
        this.tableName = tableName;
        this.familyName = familyName;
        this.columnName = columnName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = HbaseClient.getHbaseConn();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(LogEntity value, Context context) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        int rowKey = value.getUserId();
        Increment increment = new Increment(Bytes.toBytes(rowKey));
        increment.addColumn(
                Bytes.toBytes(familyName),
                Bytes.toBytes(columnName),
                1L
        );
        table.increment(increment);
    }
}
