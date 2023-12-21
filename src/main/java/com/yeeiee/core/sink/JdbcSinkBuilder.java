package com.yeeiee.core.sink;

import com.yeeiee.core.bean.JdbcOptions;
import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import com.yeeiee.utils.ReflectUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.utils.ClassDataTypeConverter;

import java.util.stream.Collectors;


@Slf4j
public abstract class JdbcSinkBuilder<IN> implements SinkBuilder<IN> {
    /**
     * @return jdbc 连接信息
     */
    public abstract JdbcOptions jdbcOptions();

    @SneakyThrows(BasicException.class)
    @Override
    public void build(Context context, DataStream<IN> input) {
        val jdbcOptions = jdbcOptions();
        log.info("Use JdbcSink config: {}", jdbcOptions);

        val tableStream = context.getTableStream();
        val statementSet = context.getStatementSet();

        // 注册sink表
        val createJdbcTableSql = getCreateJdbcTableSql(input, jdbcOptions);
        log.info("Use JdbcSink create table sql: {}", createJdbcTableSql);
        tableStream.executeSql(createJdbcTableSql);

        // 执行插入, 合并sink
        val t = tableStream.fromDataStream(input);
        statementSet.addInsert(jdbcOptions.getTableName(), t);
    }

    /**
     * 获取创建jdbc表语句
     *
     * @param input       input data stream, 只能是java bean类型
     * @param jdbcOptions jdbc连接信息
     * @return 建表语句
     * @throws BasicException generic type not pojo
     */
    private String getCreateJdbcTableSql(DataStream<?> input, JdbcOptions jdbcOptions) throws BasicException {
        // 只能是java bean类型
        val inputType = input.getType();
        if (!(inputType instanceof PojoTypeInfo)) {
            throw new BasicException("Use JdbcSink generic can only support POJO types ...");
        }

        // 获取主键列表
        val inputClass = input.getType().getTypeClass();
        val keyFieldNames = ReflectUtil.getKeyFieldNames(inputClass);
        val primaries = keyFieldNames.f0;
        val primaryKeys = String.join(",", primaries);

        // 获取字段名和类型, 字段顺序会有影响, 应该保持和mysql一致
        val pojoTypeInfo = (PojoTypeInfo<?>) inputType;
        val filedNames = keyFieldNames.f1;
        val columnNamesWithType = filedNames.stream().map(fieldName -> {
            val typeClass = pojoTypeInfo.getTypeAt(fieldName).getTypeClass();
            val fieldType = ClassDataTypeConverter.extractDataType(typeClass).orElse(DataTypes.STRING());
            return fieldName + " " + fieldType;
        }).collect(Collectors.joining(","));

        val sql = new StringBuilder();
        sql.append("CREATE TABLE ")
                .append(jdbcOptions.getTableName())
                .append(" (")
                .append(columnNamesWithType);

        // 如果主键不为空, 则添加
        if (!primaries.isEmpty()) {
            sql.append(", ")
                    .append("PRIMARY KEY (")
                    .append(primaryKeys)
                    .append(") NOT ENFORCED ");
        }

        sql.append(") WITH ( 'connector' = 'jdbc', 'url' = '")
                .append(jdbcOptions.getUrl())
                .append("', 'driver' = '")
                .append(jdbcOptions.getDriverName())
                .append("', 'table-name' = '")
                .append(jdbcOptions.getTableName())
                .append("', 'username' = '")
                .append(jdbcOptions.getUsername())
                .append("', 'password' = '")
                .append(jdbcOptions.getPassword())
                .append("' )");

        return sql.toString();
    }
}
