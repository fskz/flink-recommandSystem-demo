package com.demo.sink;

import com.demo.client.HBaseConnection;
import com.demo.util.Property;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

public abstract class HBaseSink<IN> extends RichSinkFunction<IN> {
    private final String tableName;
    private transient org.apache.hadoop.conf.Configuration configuration;
    protected transient Table table;

    public HBaseSink(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", Property.getStrValue("hbase.rootdir"));
        configuration.set("hbase.zookeeper.quorum", Property.getStrValue("hbase.zookeeper.quorum"));
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.client.scanner.timeout.period", Property.getStrValue("hbase.client.scanner.timeout.period"));
        configuration.set("hbase.rpc.timeout", Property.getStrValue("hbase.rpc.timeout"));
        table = HBaseConnection.getConnection(configuration).getTable(TableName.valueOf(tableName));
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            table.close();
        }
    }
}
