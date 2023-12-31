package com.yeeiee.utils;

import lombok.experimental.UtilityClass;
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;

@UtilityClass
public class CommonUtil {
    /**
     * 下划线转小驼峰
     *
     * @param str 输入字段名
     * @return 驼峰命名
     */
    public String underToLowerCamel(String str) {
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, str.toLowerCase());
    }

    /**
     * 小驼峰转下划线
     *
     * @param str 输入字段名
     * @return 下划线命名
     */
    public String lowerCamelToUnder(String str) {
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str);
    }

    /**
     * 大驼峰转下划线
     *
     * @param str 输入字段名
     * @return 下划线命名
     */
    public String upperCamelToUnder(String str) {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, str);
    }

    /**
     * 获取完整的 catalog.database.tableName
     *
     * @param catalogName  catalog
     * @param databaseName 库名
     * @param tableName    表名
     * @return full table name
     */
    public String getFullTableName(String catalogName, String databaseName, String tableName) {
        return String.format("`%s`.`%s`.`%s`", catalogName, databaseName, tableName);
    }
}
