package com.yeeiee.core.sink;

import com.yeeiee.core.catalog.CatalogTable;
import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import com.yeeiee.utils.CommonUtil;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 字段要对齐
 */
@Slf4j
public abstract class JdbcSinkBuilder<OUT> implements SinkBuilder<OUT>, CatalogTable {

    @SneakyThrows(BasicException.class)
    @Override
    public void build(Context context, DataStream<OUT> input) {
        this.validate();
        log.info("Use JdbcSink validated success");

        // 只能是java bean类型
        val inputType = input.getType();
        if (!(inputType instanceof PojoTypeInfo)) {
            throw new BasicException("Use JdbcSink generic can only support POJO types ...");
        }

        val tableStream = context.getTableStream();
        val statementSet = context.getStatementSet();
        val table = tableStream.fromDataStream(input);

        val fullTableName = CommonUtil.getFullTableName(this.catalog(), this.databaseWithTableName());
        log.info("Use JdbcSink will insert to table: {}", fullTableName);

        if (this.isCombineSink()) {
            log.info("Use JdbcSink will combine sink");
            statementSet.addInsert(fullTableName, table);
        } else {
            log.info("Use JdbcSink will single sink");
            table.insertInto(fullTableName);
        }

    }
}
