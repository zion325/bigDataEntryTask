package org.example;

import java.sql.*;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.*;
import org.apache.spark.sql.jdbc.JdbcDialects;


import java.time.Instant;
import java.util.Arrays;


public class Cumulative {
    public static void main(String[] args) throws AnalysisException {
        // 设置 Hive 连接参数
        String hiveUrl = "jdbc:hive2://localhost:10000/user_behavior";
        String hiveUser = "hive";
        String hivePassword = "123456";

        // 设置 MySQL 连接参数
        String mysqlUrl = "jdbc:mysql://localhost:3306/entryTask";
        String mysqlUser = "hive";
        String mysqlPassword = "123456";

        // 创建 SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Hive to MySQL")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();

        // 坑点：spark读hive表的时候，可能会自动加单/双引号 或者 反引号，这两句是去掉他们
        HiveSqlDialect hiveSqlDialect = new HiveSqlDialect();
        JdbcDialects.registerDialect(hiveSqlDialect);

        // 从 Hive 中读取数据
        Dataset<Row> hiveData = spark.read()
                .format("jdbc")
                .option("driver", "org.apache.hive.jdbc.HiveDriver")
                .option("url", hiveUrl)
                .option("dbtable", "user_behavior")
                .option("user", hiveUser)
                .option("password", hivePassword)
                .load();


        // 坑点：读出来读数据列名会重命名，加表名前缀，例如：userID --> tableName.userID
        // 手动给他去掉
        for (String oldName : hiveData.columns()) {
            String newName = oldName.replace("user_behavior" + ".", "").replace("'", "");
            hiveData = hiveData.withColumnRenamed(oldName, newName);
        }

        hiveData.createTempView("a");


        // 执行指标计算，这里举例计算单位小时内的 用户量uv，购买量buy_count，总金额amount，
        // 可以做成定时任务，以每天执行为例
        long currentTimeSeconds = Instant.now().getEpochSecond();
        // 计算一天前的时间戳（秒）
        long startTime = currentTimeSeconds - 3600*24;
        System.out.println("start: " + startTime);
        Dataset<Row> result = spark.sql("SELECT \n" +
                "CAST(t1.dt AS string) AS dt, \n" +
                "COUNT(t1.user_id) AS uv, \n" +
                "SUM(t1.buy_cnt) AS buy_count, \n" +
                "SUM(t1.amount) AS amount \n" +
                "FROM ( \n" +
                "SELECT \n" +
                "SUBSTR(FROM_UTC_TIMESTAMP(cast(order_time as timestamp),'PRC'), 1, 13) AS dt, \n" +
                "cast(user_id as string) as user_id, \n" +
                "COUNT(1) AS buy_cnt, \n" +
                "SUM(amount) AS amount \n" +
                "FROM  a \n" +
                "WHERE cast(order_time AS bigint) >= "+ startTime +" \n" +
                "GROUP BY FROM_UTC_TIMESTAMP(cast(order_time as timestamp),'PRC'), user_id \n" +
                ") t1 \n" +
                "GROUP BY CAST(t1.dt AS string)" +
                "order by cast(dt AS timestamp) desc" +
                ";");

        result.show();
        // 将结果保存到 MySQL
        result.write().format("jdbc")
                .option("url", mysqlUrl)
                .option("user", mysqlUser)
                .option("password", mysqlPassword)
                .option("dbtable", "cumulative")
                .mode(SaveMode.Append)
                .save();
        // 关闭 SparkSession
        spark.stop();

    }


    public static class HiveSqlDialect extends JdbcDialect {

        @Override
        public boolean canHandle(String url){
            return url.startsWith("jdbc:hive2");
        }

        @Override
        public String quoteIdentifier(String colName) {
            return colName.replace("\"","");
        }

    }
}





