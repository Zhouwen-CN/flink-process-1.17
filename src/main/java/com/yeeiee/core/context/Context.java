package com.yeeiee.core.context;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Slf4j
@Getter
public class Context {
    private String jobName;
    private StreamExecutionEnvironment dataStream;
    private StreamTableEnvironment tableStream;
    private StatementSet statementSet;

    private Context(@NonNull String jobName, StreamExecutionEnvironment dataStream,StreamTableEnvironment tableStream, StatementSet statementSet){
        this.jobName = jobName;
        this.dataStream = dataStream;
        this.tableStream = tableStream;
        this.statementSet = statementSet;
    }
    public static class ContextBuilder{
        private String jobName;
        private ContextBuilder(){}

        public ContextBuilder setJobClass(Class<?> jobClass){
            this.jobName = jobClass.getSimpleName();
            return this;
        }

        public Context build(){
            return build(null);
        }

        public Context build(JobConfig jobConfig){
            val jobName = this.jobName;
            val config = JobConfig.getJobConfig(jobName, jobConfig);
            log.info("Use jobConfig: {}",config);
            val dataStream = StreamExecutionEnvironment.getExecutionEnvironment();
            dataStream.setRuntimeMode(config.getRuntimeMode());
            dataStream.enableCheckpointing(config.getCheckpointInterval());
            val checkpointConfig = dataStream.getCheckpointConfig();
            checkpointConfig.setCheckpointingMode(config.getCheckpointMode());
            checkpointConfig.setCheckpointStorage(config.getCheckpointStorage());
            checkpointConfig.setCheckpointTimeout(config.getCheckpointTimeout());
            checkpointConfig.setMinPauseBetweenCheckpoints(config.getMinPauseBetweenCheckpoints());
            checkpointConfig.setMaxConcurrentCheckpoints(config.getMaxConcurrentCheckpoints());
            checkpointConfig.setTolerableCheckpointFailureNumber(config.getTolerableCheckpointFailureNumber());
            dataStream.setStateBackend(config.getStateBackend());
            dataStream.setRestartStrategy(config.getRestartStrategy());
            val tableStream = StreamTableEnvironment.create(dataStream);
            val statementSet = tableStream.createStatementSet();
            log.info("context init success");
            return new Context(jobName,dataStream,tableStream,statementSet);
        }
    }

    public static ContextBuilder builder(){
        return new ContextBuilder();
    }
}
