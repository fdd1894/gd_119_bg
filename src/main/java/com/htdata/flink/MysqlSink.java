package com.htdata.flink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 从kafka读取数据写入mysql
 * 自定义sink
 */
public class MysqlSink extends
        RichSinkFunction<Tuple3<Integer, String, Integer>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "root";
    String password = "123456";
    String drivername = "com.mysql.jdbc.Driver";
    String dburl = "jdbc:mysql://你的mysqlip:3306/flink";

    @Override
    public void invoke(Tuple3<Integer, String, Integer> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into person(card,name,phone) values(?,?,?)"; //mysql存在表person 有3个字段 card,name,phone
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, value.f0);
        preparedStatement.setString(2, value.f1);
        preparedStatement.setInt(3, value.f2);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
