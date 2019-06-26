package com.leone.bigdata.common.util;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leone.bigdata.common.entity.Http;
import com.leone.bigdata.common.entity.Ss;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-22
 **/
public class DBUtil {

    public static final DBUtil dbUtil = new DBUtil();

    private static LinkedList<Connection> pool = new LinkedList<>();

    private DBUtil() {
    }

    public static DBUtil getInstance() {
        return dbUtil;
    }

    /**
     * @param config
     * @param clazz
     */
    public List<String> select(Config config, String sql, Class<?> clazz) {
        Connection conn;
        if (pool.size() < 1) {
            conn = getConnection(config);
            pool.add(conn);
        } else {
            conn = pool.get(0);
        }
        List<String> list = new ArrayList<>();
        try {
            final Statement statement = conn.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                StringBuilder sb = new StringBuilder("{");
                Map<String, Class> filedTypes = getClassFiledType(clazz);
                filedTypes.forEach((k, v) -> {
                    try {
                        switch (v.getName()) {
                            case "java.lang.String":
                                String s = resultSet.getString(k);
                                sb.append("\"").append(k).append("\"").append(":").append("\"").append(s == null ? "" : s).append("\"").append(",");
                                break;
                            case "java.lang.Byte":
                            case "byte":
                                Byte b = resultSet.getByte(k);
                                sb.append("\"").append(k).append("\"").append(":").append(b == null ? 0 : b).append(",");
                                break;
                            case "java.lang.Short":
                            case "short":
                                Short sh = resultSet.getShort(k);
                                sb.append("\"").append(k).append("\"").append(":").append(sh == null ? 0 : sh).append(",");
                                break;
                            case "java.lang.Integer":
                            case "int":
                                Integer i = resultSet.getInt(k);
                                sb.append("\"").append(k).append("\"").append(":").append(i == null ? 0 : i).append(",");
                                break;
                            case "java.lang.Long":
                            case "long":
                                Long l = resultSet.getLong(k);
                                sb.append("\"").append(k).append("\"").append(":").append(l == null ? 0 : l).append(",");
                                break;
                            case "java.lang.Float":
                            case "float":
                                Float f = resultSet.getFloat(k);
                                sb.append("\"").append(k).append("\"").append(":").append(f == 0 ? "" : f).append(",");
                                break;
                            case "java.lang.Double":
                            case "double":
                                Double d = resultSet.getDouble(k);
                                sb.append("\"").append(k).append("\"").append(":").append(d == null ? 0 : d).append(",");
                                break;
                            case "java.lang.Boolean":
                            case "boolean":
                                Boolean bool = resultSet.getBoolean(k);
                                sb.append("\"").append(k).append("\"").append(":").append(bool == null ? false : bool).append(",");
                                break;
                            default:
                                break;
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                list.add(sb.substring(0, sb.length() - 1) + "}");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }


    /**
     * 获取数据库连接
     *
     * @return
     */
    private static Connection getConnection(Config config) {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String dbUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false", config.ip, config.port, config.db);
            conn = DriverManager.getConnection(dbUrl, config.username, config.password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 反射获取 Class 的属性类型
     *
     * @param clazz
     * @return
     */
    private Map<String, Class> getClassFiledType(Class<?> clazz) {
        if (Objects.isNull(clazz)) {
            return null;
        }
        Field[] fields = clazz.getDeclaredFields();
        Field.setAccessible(fields, true);
        Map<String, Class> types = new TreeMap<>();
        for (Field field : fields) {
            Class<?> type = field.getType();
            types.put(field.getName(), type);
        }
        return types;
    }


    /**
     * 关闭连接
     *
     * @param connection
     */
    private static void close(Connection connection, Statement statement, ResultSet resultSet) {
        try {
            if (Objects.nonNull(resultSet)) {
                resultSet.close();
            }
            if (Objects.nonNull(statement)) {
                statement.close();
            }
            if (Objects.nonNull(connection)) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param sql
     */
    public static boolean execute(Config config, String sql) {
        Connection connection;
        if (pool.size() < 1) {
            connection = getConnection(config);
            pool.add(connection);
        } else {
            connection = pool.get(0);
        }
        try {
            Statement statement = connection.createStatement();
            return statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static class Config {
        String db;
        String ip;
        String username;
        String password;
        Integer port;

        public Config() {
        }

        public Config(String db, String ip, String username, String password, Integer port) {
            this.db = db;
            this.ip = ip;
            this.username = username;
            this.password = password;
            this.port = port;
        }
    }

}
