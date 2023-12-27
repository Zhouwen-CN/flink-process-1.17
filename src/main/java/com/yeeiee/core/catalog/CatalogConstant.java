package com.yeeiee.core.catalog;

import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * register catalog list
 */
@UtilityClass
public class CatalogConstant {
    public Map<String, Catalog> catalogs = new HashMap<>();
    public JdbcCatalog jdbcCatalog;

    static {
        val classLoader = Thread.currentThread().getContextClassLoader();

        jdbcCatalog = new JdbcCatalog(classLoader,
                "jdbc_catalog",
                "chen_test",
                "root",
                "123",
                "jdbc:mysql://127.0.0.1:3306");

        catalogs.put(jdbcCatalog.getName(), jdbcCatalog);
    }
}
