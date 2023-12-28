package com.yeeiee.core.catalog;

import lombok.experimental.UtilityClass;
import org.apache.flink.table.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * register catalog list
 */
@UtilityClass
public class CatalogConstant {
    public Map<String, Catalog> catalogs = new HashMap<>();
}
