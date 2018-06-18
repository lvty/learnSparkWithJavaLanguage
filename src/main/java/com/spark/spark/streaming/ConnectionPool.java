package com.spark.spark.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;//静态的connection队列

    static{
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection(){

        if(connectionQueue == null){
            connectionQueue = new LinkedList<Connection>();
            for (int i = 0; i < 10; i++) {
                //创建10个连接
                try {
                    connectionQueue.offer(DriverManager.getConnection(
                            "jdbc:mysql://node1:3306/mydb","",""));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection c){
        connectionQueue.offer(c);
    }
}
