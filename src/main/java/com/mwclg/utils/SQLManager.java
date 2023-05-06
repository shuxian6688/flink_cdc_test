package com.mwclg.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLManager {
    static Connection connection;
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        //1. 注册加载驱动
        Class .forName("com.mysql.jdbc.Driver");
        //2.获得数据库的连接
        //(1).连接mysql的url地址
        String url="jdbc:mysql://%s:%d/%s".format(GetConfInfoUtils.getProperty("doris_host"), Integer.parseInt(GetConfInfoUtils.getProperty("doris_port")), GetConfInfoUtils.getProperty("sync_config_db"));
        //(2).连接mysql的用户名
        String username="root";
        //(3).连接mysql的密码
        String pwd="123456";
        Connection con=DriverManager.getConnection(url, username, pwd);
        System.out.println("MySQL连接成功！"+con);
        return con;
    }
}
