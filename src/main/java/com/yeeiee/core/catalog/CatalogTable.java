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
    default String catalog() {
        return "default_catalog";
    }

    /**
     * database
     *
     * @return default_database
     */
    default String database() {
        return "default_database";
    }

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
        val catalogName = this.catalog();
        if ("default_catalog".equals(catalogName)) {
            return;
        }
        val catalogs = CatalogConstant.catalogs;
        if (!catalogs.containsKey(catalogName)) {
            throw new BasicException("Can not find catalog: " + catalogName);
        }
        val currentCatalog = catalogs.get(catalogName);
        val databaseName = this.database();
        if (!currentCatalog.databaseExists(databaseName)) {
            throw new BasicException("Can not find database " + databaseName + " in " + catalogName);
        }
    }
}
