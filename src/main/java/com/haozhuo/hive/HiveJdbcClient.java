package com.haozhuo.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/**
 * https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-JDBC
 */
public class HiveJdbcClient {
    private static String driverName="org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            //replace "hive" here with the name of the user the queries should run as
            Connection con = DriverManager.getConnection("jdbc:hive2://192.168.1.150:10000/default", "hadoop", "");
            Statement stmt = con.createStatement();
            /*String tableName = "testHiveDriverTable";
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName + " (key int, value string)");*/
            // show tables
            /*String sql = "show tables '" + tableName + "'";*/
            //String sql = "delete from users where name = '11'";
            //String sql = "ALTER TABLE users  COMPACT 'major'";
            String sql = "ALTER TABLE users  COMPACT 'major'";
             stmt.executeQuery(sql);
       /*     while (res.next()) {
                System.out.println(res.getInt(1) + "\t" + res.getString(2));
            }*/
            /*// describe table
            sql = "describe " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }*/

            // load data into table
            // NOTE: filepath has to be local to the hive server
            // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
            /*String filepath = "/tmp/a.txt";
            sql = "load data local inpath '" + filepath + "' into table " + tableName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);
*/
            // select * query
           /* sql = "select * from " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
            }
*/
            // regular hive query
            /*sql = "select count(1) from " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }*/
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
