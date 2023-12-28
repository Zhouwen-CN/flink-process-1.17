package com.yeeiee;

import lombok.val;

public class JdbcCatalogTest extends AbstractTest {

    public void testJdbcCatalog() {
        val tableStream = context.getTableStream();

        tableStream.executeSql("CREATE TABLE print_sink ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'print' \n" +
                ");\n");

//        tableEnv.executeSql("insert into `jdbc_catalog`.`chen_test`.water_sensor select * from `default_catalog`.`default_data`.source");
        tableStream.sqlQuery("select * from `jdbc_catalog`.`chen_test`.water_sensor")
                .execute()
                .print();
    }
}
