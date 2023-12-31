package com.yeeiee.core.flow;

import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.table.operations.ModifyOperation;

import java.util.List;

@Slf4j
public abstract class AbstractFlow implements Flow {
    protected void submit(Context context) throws BasicException {
        try {
            val name = context.getJobName();
            log.info("context will submit with name: {}", name);
            context.getTableStream().getConfig().getConfiguration().setString("pipeline.name", name);
            // 只有业务使用过flink StatementSet sql的时候才执行
            ifPresentStatementSetExecute(context);
            // 只有dataStream级别, 有addSink|sinkTo操作才会执行避免sql和dataStream混合开发, 提交多余的执行链
            ifPresentSinkExecute(context);
        } catch (Exception e) {
            throw new BasicException("context submit error", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void ifPresentStatementSetExecute(Context context) throws Exception {
        val operationsField = context.getStatementSet().getClass().getSuperclass().getDeclaredField("operations");
        operationsField.setAccessible(true);
        val operations = ((List<ModifyOperation>) operationsField.get(context.getStatementSet()));
        if (operations != null && !operations.isEmpty()) {
            log.info("operations size is {} statementSet execute", operations.size());
            context.getStatementSet().execute();
        }
    }

    @SuppressWarnings("unchecked")
    private void ifPresentSinkExecute(Context context) throws Exception {
        val transformationsField = StreamExecutionEnvironment.class.getDeclaredField("transformations");
        transformationsField.setAccessible(true);
        val transformations = (List<Transformation<?>>) transformationsField.get(context.getDataStream());
        if (transformations != null && !transformations.isEmpty()) {
            val count = transformations.stream().filter(transformation -> transformation instanceof LegacySinkTransformation || transformation instanceof SinkTransformation).count();
            if (count > 0) {
                log.info("transformations size is {} dataStream execute", count);
                context.getDataStream().execute(context.getJobName());
            }
        }
    }
}
