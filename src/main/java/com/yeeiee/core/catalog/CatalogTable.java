package com.yeeiee.core.catalog;

import com.yeeiee.exception.BasicException;
import lombok.SneakyThrows;
import lombok.val;

public interface CatalogTable {
    /**
     * catalog
     *
     * @return default_catalog
     */
    String catalog();

    /**
     * database
     *
     * @return default_database
     */
    String databaseWithTableName();

    /**
     * 是否合并sink
     *
     * @return default true
     */
    default boolean isCombineSink() {
        return true;
    }

    /**
     * 检查 catalog 和 database 是否存在
     */
    @SneakyThrows(BasicException.class)
    default void validate() {
        val catalogs = CatalogConstant.catalogs;
        val currentCatalog = this.catalog();
        if (catalogs.containsKey(currentCatalog)) {
            val catalog = catalogs.get(currentCatalog);
            val currentDatabaseWithTableName = this.databaseWithTableName();
            if (!currentDatabaseWithTableName.contains(".")) {
                throw new BasicException("DatabaseWithTableName require type database.tableName, but " + currentDatabaseWithTableName);
            }
            val currentDatabase = currentDatabaseWithTableName.split("\\.")[0];
            if (!catalog.databaseExists(currentDatabase)) {
                throw new BasicException("Can not find database " + currentDatabase + " in " + catalog);
            }
        } else {
            throw new BasicException("Can not find catalog " + currentCatalog);
        }

    }
}
