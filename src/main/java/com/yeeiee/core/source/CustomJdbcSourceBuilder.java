package com.yeeiee.core.source;

import com.yeeiee.core.catalog.CatalogConstant;
import com.yeeiee.core.catalog.CatalogTable;
import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import com.yeeiee.utils.ReflectUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class CustomJdbcSourceBuilder<IN> extends RichParallelSourceFunction<IN> implements SourceBuilder<IN>, CatalogTable {
    /**
     * @return 查询间隔, 秒
     */
    protected abstract Integer interval();

    /**
     * 连接参数, 因为 catalog 里面的 baseUrl不能加参数
     *
     * @return 默认为空
     */
    protected String connectParam() {
        return null;
    }

    private boolean isRunning = true;

    private Connection connection;

    private CountDownLatch countDownLatch;

    // 父类泛型
    private Class<?> superClassGeneric;

    // 字段列表
    private String stringFields;

    private String getFullConnectUrl(String baseUrl) {
        val databaseWithTableName = this.databaseWithTableName();
        val databaseName = databaseWithTableName.split("\\.")[0];
        var connectParam = this.connectParam();
        connectParam = connectParam == null || connectParam.trim().isEmpty() ? "" : "&" + connectParam;
        return baseUrl + databaseName + connectParam;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 校验
        this.validate();

        // 所有字段都需要有set方法
        superClassGeneric = ReflectUtil.getSuperClassT(this);
        if (!ReflectUtil.checkBeanHasSetMethods(superClassGeneric)) {
            throw new BasicException("Use CustomJdbcSource all fields of a generic object require a set method");
        }

        // 获取字段列表, 字段列表不能为空
        val fieldList = ReflectUtil.getFieldNames(superClassGeneric);
        if (fieldList.isEmpty()) {
            throw new BasicException("Use CustomJdbcSource the field of a generic object cannot be empty");
        }
        stringFields = String.join(", ", fieldList);

        // 获取当前的catalog
        val catalog = CatalogConstant.catalogs.get(this.catalog());
        if (!(catalog instanceof JdbcCatalog)) {
            throw new BasicException("Use CustomJdbcSource the catalog require JdbcCatalog");
        }
        val jdbcCatalog = ((JdbcCatalog) catalog);
        val internal = jdbcCatalog.getInternal();

        // 从catalog中获取连接信息
        val baseUrl = internal.getBaseUrl();
        val username = internal.getUsername();
        val password = internal.getPassword();
        val fullConnectUrl = this.getFullConnectUrl(baseUrl);
        log.info("Use CustomJdbcSource config: url={}, username={}, password={}, tableName={}", fullConnectUrl, username, password, this.databaseWithTableName());
        connection = DriverManager.getConnection(fullConnectUrl, username, password);
        countDownLatch = new CountDownLatch(1);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run(SourceContext<IN> sourceContext) throws Exception {
        while (isRunning) {
            val statement = connection.createStatement();
            val querySql = "select " + stringFields + " from " + this.databaseWithTableName();

            val resultSet = statement.executeQuery(querySql);
            val metaData = resultSet.getMetaData();
            val columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                // 创建泛型对象
                val bean = (IN) superClassGeneric.newInstance();

                // 给泛型对象赋值
                for (int i = 1; i < columnCount + 1; i++) {
                    // 获取列名
                    val columnName = metaData.getColumnName(i);
                    // 获取列值
                    val value = resultSet.getObject(i);
                    // 给泛型对象赋值
                    BeanUtils.setProperty(bean, columnName, value);
                }
                sourceContext.collect(bean);
            }

            //关闭资源
            resultSet.close();
            statement.close();

            countDownLatch.await(interval(), TimeUnit.SECONDS);
        }
    }

    @Override
    public void cancel() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            countDownLatch.countDown();
        }
        isRunning = false;
    }

    @Override
    public DataStream<IN> build(Context context) {
        return context.getDataStream().addSource(this);
    }
}
