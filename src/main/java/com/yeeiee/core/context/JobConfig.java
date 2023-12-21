package com.yeeiee.core.context;

import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.val;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;

@ToString
public final class JobConfig {
    // todo require hdfs path, recent use local path
    private static final String DEFAULT_CHECKPOINT_STORAGE_PATH_PREFIX = "file:///D:\\tmp\\flink\\";

    public JobConfig(String jobName) {
        this.checkpointStorage = DEFAULT_CHECKPOINT_STORAGE_PATH_PREFIX + jobName;
    }

    /**
     * 检查点保存位置
     */
    private @Getter String checkpointStorage;
    /**
     * 检查点模式
     */
    private @Getter CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;
    /**
     * 检查点间隔
     */
    private @Getter Long checkpointInterval = 180000L;
    /**
     * 检查点超时
     */
    private @Getter Long checkpointTimeout = 180000L;
    /**
     * 上一个checkpoint结束之后,多久才能发出另一个checkpoint
     * 当设置此值时 maxConcurrentCheckpoints 为 1
     */
    private @Getter Long minPauseBetweenCheckpoints = 500L;
    /**
     * 检查点最大并发数量
     */
    private @Getter Integer maxConcurrentCheckpoints = 1;
    /**
     * 可容忍检查点失败次数
     */
    private @Getter Integer tolerableCheckpointFailureNumber = 3;
    /**
     * 状态后端
     */
    private @Getter StateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
    /**
     * 重启策略
     */
    private @Getter RestartStrategies.FailureRateRestartStrategyConfiguration restartStrategy = RestartStrategies.failureRateRestart(5, Time.minutes(5), Time.seconds(10));


    public void setCheckpointStorage(String checkpointStorage) {
        if (checkpointStorage != null) {
            this.checkpointStorage = checkpointStorage;
        }
    }

    public void setCheckpointMode(CheckpointingMode checkpointMode) {
        if (checkpointMode != null) {
            this.checkpointMode = checkpointMode;
        }
    }

    public void setCheckpointInterval(Long checkpointInterval) {
        if (checkpointInterval != null) {
            this.checkpointInterval = checkpointInterval;
        }
    }

    public void setCheckpointTimeout(Long checkpointTimeout) {
        if (checkpointTimeout != null) {
            this.checkpointTimeout = checkpointTimeout;
        }
    }

    public void setMinPauseBetweenCheckpoints(Long minPauseBetweenCheckpoints) {
        if (minPauseBetweenCheckpoints != null) {
            this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        }
    }

    public void setMaxConcurrentCheckpoints(Integer maxConcurrentCheckpoints) {
        if (maxConcurrentCheckpoints != null) {
            this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
        }
    }

    public void setTolerableCheckpointFailureNumber(Integer tolerableCheckpointFailureNumber) {
        if (tolerableCheckpointFailureNumber != null) {
            this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
        }
    }

    public void setStateBackend(StateBackend stateBackend) {
        if (stateBackend != null) {
            this.stateBackend = stateBackend;
        }
    }

    public void setRestartStrategy(RestartStrategies.FailureRateRestartStrategyConfiguration restartStrategy) {
        if (restartStrategy != null) {
            this.restartStrategy = restartStrategy;
        }
    }

    public static JobConfig getJobConfig(@NonNull String jobName, JobConfig jobConfig) {
        if (jobConfig == null) {
            return new JobConfig(jobName);
        }
        val config = new JobConfig(jobName);
        config.setCheckpointMode(jobConfig.getCheckpointMode());
        config.setCheckpointInterval(jobConfig.getCheckpointInterval());
        config.setCheckpointStorage(jobConfig.getCheckpointStorage());
        config.setCheckpointTimeout(jobConfig.getCheckpointTimeout());
        config.setMinPauseBetweenCheckpoints(jobConfig.getMinPauseBetweenCheckpoints());
        config.setMaxConcurrentCheckpoints(jobConfig.getMaxConcurrentCheckpoints());
        config.setTolerableCheckpointFailureNumber(jobConfig.getTolerableCheckpointFailureNumber());
        config.setStateBackend(jobConfig.getStateBackend());
        config.setRestartStrategy(jobConfig.getRestartStrategy());
        return config;
    }
}
