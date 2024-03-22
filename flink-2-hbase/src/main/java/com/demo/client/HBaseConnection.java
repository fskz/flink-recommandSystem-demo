package com.demo.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConnection {
    private static Connection connection;

    public static synchronized Connection getConnection(Configuration config) throws IOException {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionFactory.createConnection(config);
        }
        return connection;
    }
}
