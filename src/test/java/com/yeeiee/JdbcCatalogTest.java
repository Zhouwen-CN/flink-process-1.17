package com.yeeiee;

import lombok.val;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JdbcCatalogTest {
    public static void main(String[] args) throws Exception {
        val env = StreamExecutionEnvironment.getExecutionEnvironment();
        val tableEnv = StreamTableEnvironment.create(env);

        val classLoader = Thread.currentThread().getContextClassLoader();
        val jdbcCatalog = new JdbcCatalog(classLoader,
                "jdbc_catalog",
                "chen_test",
                "root",
                "123",
                "jdbc:mysql://127.0.0.1:3306");

        tableEnv.registerCatalog(jdbcCatalog.getName(),jdbcCatalog);

        tableEnv.executeSql("CREATE TABLE print_sink ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'print' \n" +
                ");\n");


//        tableEnv.executeSql("insert into `jdbc_catalog`.`chen_test`.water_sensor select * from `default_catalog`.`default_data`.source");
        val table = tableEnv.sqlQuery("select * from `jdbc_catalog`.`chen_test`.water_sensor");

        tableEnv.toChangelogStream(table).print();

        env.execute();

    }
}
