package com.yeeiee.core.source;

import com.yeeiee.core.bean.JdbcOptions;
import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import com.yeeiee.utils.ReflectUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class CustomJdbcSourceBuilder<IN> extends RichParallelSourceFunction<IN> implements SourceBuilder<IN> {
    /**
     * @return jdbc 连接信息
     */
    public abstract JdbcOptions jdbcOptions();

    /**
     * @return 查询间隔, 秒
     */
    public abstract Integer interval();

    private boolean isRunning = true;

    private Connection connection;

    private CountDownLatch countDownLatch;

    // 父类泛型
    private Class<?> superClassGeneric;

    // 字段列表
    private String stringFields;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 所有字段都需要有set方法
        superClassGeneric = ReflectUtil.getSuperClassT(this);
        if (!ReflectUtil.checkBeanHasSetMethods(superClassGeneric)) {
            throw new BasicException("Use CustomJdbcSource all fields of a generic object require a set method");
        }

        // 获取字段列表, 字段列表不能为空
        val fieldList = ReflectUtil.getKeyFieldNames(superClassGeneric).f1;
        if (fieldList.isEmpty()) {
            throw new BasicException("Use CustomJdbcSource the field of a generic object cannot be empty");
        }
        stringFields = String.join(", ", fieldList);

        // 创建jdbc连接
        val jdbcOptions = jdbcOptions();
        log.info("Use CustomJdbcSource config: {}", jdbcOptions);
        Class.forName(jdbcOptions.getDriverName());
        connection = DriverManager.getConnection(jdbcOptions.getUrl(), jdbcOptions.getUsername(), jdbcOptions.getPassword());
        countDownLatch = new CountDownLatch(1);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run(SourceContext<IN> sourceContext) throws Exception {
        while (isRunning) {
            val statement = connection.createStatement();
            val tableName = jdbcOptions().getTableName();
            val querySql = "select " + stringFields + " from " + tableName;

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
