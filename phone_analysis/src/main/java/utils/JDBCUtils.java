package utils;

import java.sql.*;

/**
 * @author Andy
 */
public class JDBCUtils {
    /**
     * 注意：
     * MYSQL_URL:记得使用自己的库、记得创建这个库
     *
     */
    private static final String PG_DRIVER_CLASS = "com.postgresql.jdbc.Driver";
    private static final String PG_URL = "jdbc:postgresql://bigdata121:3306/lost?useUnicode=true&characterEncoding=UTF-8";
    private static final String PG_USERNAME = "lostopr";
    private static final String PG_PASSWORD = "paic134";

    /**
     * 实例化JDBC连接器
     * @return
     */
    public static Connection getConnection(){
        try {
            Class.forName(PG_DRIVER_CLASS);
            return DriverManager.getConnection(PG_URL, PG_USERNAME, PG_PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 释放连接器
     * @param connection
     * @param statement
     * @param resultSet
     */
    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null || !resultSet.isClosed()) {
                resultSet.close();
            }
            if (statement != null || !statement.isClosed()) {
                statement.close();
            }
            if (connection != null || !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
