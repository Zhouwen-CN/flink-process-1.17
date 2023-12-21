package com.yeeiee.core.bean;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class JdbcOptions {
    /**
     * 驱动全类名
     */
    private String driverName;

    /**
     * 连接url
     */
    private String url;

    /**
     * 连接用户名
     */
    private String username;

    /**
     * 连接密码
     */
    private String password;

    /**
     * 表名称
     */
    private String tableName;

}
