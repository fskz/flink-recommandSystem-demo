package com.demo.sink;

import com.demo.client.HBaseConnection;
import com.demo.client.HbaseClient;
import com.demo.util.Property;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

public abstract class HBaseSink<IN> extends RichSinkFunction<IN> {
    private final String tableName;
    protected transient Table table;
    protected transient Connection connection;

    public HBaseSink(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = HbaseClient.getHbaseConn();
        table = connection.getTable(TableName.valueOf(tableName));
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            table.close();
        }

        if (connection != null) {
            connection.close();
        }

    }
}
