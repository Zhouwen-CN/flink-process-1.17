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
import org.apache.flink.streaming.api.environment.CheckpointConfig;

import java.time.Duration;

@ToString
@Getter
public final class FlowConfig {
    // todo require hdfs path, recent use local path
    private static final String DEFAULT_CHECKPOINT_STORAGE_PATH_PREFIX = "file:///D:\\tmp\\flink\\";

    public FlowConfig(){}

    public FlowConfig(String jobName) {
        this.checkpointStorage = DEFAULT_CHECKPOINT_STORAGE_PATH_PREFIX + jobName;
    }

    /**
     * 检查点保存位置
     */
    private String checkpointStorage;
    /**
     * 检查点模式
     * Barrier对齐(默认): 一个Task收到所有上游同一编号的barrier之后,才会对自己的本地状态做备份
     * 精准一次: 在对齐过程中,barrier后面的数据阻塞等待(不会越过barrier)
     * 至少一次: 在对齐过程中,想到的barrier其后面的数据不阻塞,接着计算
     * 非Barrier对其(精准一次): 一个Task收到第一个barrier时,就开始执行备份
     * 先到的barrier,会将本地状态备份,其后面的数据接着计算输出
     * 未到的barrier,其前面的数据接着计算输出,同时也保存到备份中
     * 最后一个barrier到达该Task时,这个Task的备份结束
     * tips: 意思就是将先到的barrier和后到的barrier和其中间的数据一起备份
     */
    private CheckpointingMode checkpointMode = CheckpointingMode.AT_LEAST_ONCE;
    /**
     * 非对称检查点
     * 开启要求:
     * 1. checkpoint mode 必须是精准一次
     * 2. 最大并发度必须是 1
     */
    private boolean enableUnalignedCheckpoint = false;
    /**
     * 对称检查点超时时间,当对称检查点超过当前时间,启用非对称检查点
     * 需要开启非对称检查点
     */
    private Duration alignedCheckpointTimeout = Duration.ZERO;
    /**
     * 检查点间隔
     */
    private long checkpointInterval = 180000L;
    /**
     * 检查点超时
     */
    private long checkpointTimeout = 180000L;
    /**
     * 上一个checkpoint结束之后,多久才能发出另一个checkpoint
     * 当设置此值时,实际并发为 1
     */
    private long minPauseBetweenCheckpoints = 500L;
    /**
     * 检查点最大并发数量
     */
    private int maxConcurrentCheckpoints = 1;
    /**
     * 可容忍检查点失败次数
     */
    private int tolerableCheckpointFailureNumber = 3;
    /**
     * 取消作业时,是否保留checkpoint数据
     */
    private CheckpointConfig.ExternalizedCheckpointCleanup checkpointCleanup = CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
    /**
     * 状态后端
     */
    private StateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
    /**
     * 重启策略
     */
    private RestartStrategies.FailureRateRestartStrategyConfiguration restartStrategy = RestartStrategies.failureRateRestart(5, Time.minutes(5), Time.seconds(10));

    public FlowConfig setCheckpointStorage(String checkpointStorage) {
        if (checkpointStorage != null) {
            this.checkpointStorage = checkpointStorage;
        }
        return this;
    }

    public FlowConfig setCheckpointMode(CheckpointingMode checkpointMode) {
        if (checkpointMode != null && !this.enableUnalignedCheckpoint) {
            this.checkpointMode = checkpointMode;
        }
        return this;
    }

    public FlowConfig setEnableUnalignedCheckpoint(Boolean enableUnalignedCheckpoint) {
        if (enableUnalignedCheckpoint != null) {
            this.enableUnalignedCheckpoint = enableUnalignedCheckpoint;
            this.checkpointMode = CheckpointingMode.EXACTLY_ONCE;
            this.maxConcurrentCheckpoints = 1;
        }
        return this;
    }

    public FlowConfig setAlignedCheckpointTimeout(Duration alignedCheckpointTimeout) {
        if (alignedCheckpointTimeout!=null && this.enableUnalignedCheckpoint){
            this.alignedCheckpointTimeout = alignedCheckpointTimeout;
        }
        return this;
    }

    public FlowConfig setCheckpointInterval(Long checkpointInterval) {
        if (checkpointInterval != null) {
            this.checkpointInterval = checkpointInterval;
        }
        return this;
    }

    public FlowConfig setCheckpointTimeout(Long checkpointTimeout) {
        if (checkpointTimeout != null) {
            this.checkpointTimeout = checkpointTimeout;
        }
        return this;
    }

    public FlowConfig setMinPauseBetweenCheckpoints(Long minPauseBetweenCheckpoints) {
        if (minPauseBetweenCheckpoints != null) {
            this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        }
        return this;
    }

    public FlowConfig setMaxConcurrentCheckpoints(Integer maxConcurrentCheckpoints) {
        if (maxConcurrentCheckpoints != null && !this.enableUnalignedCheckpoint) {
            this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
        }
        return this;
    }

    public FlowConfig setTolerableCheckpointFailureNumber(Integer tolerableCheckpointFailureNumber) {
        if (tolerableCheckpointFailureNumber != null) {
            this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
        }
        return this;
    }

    public FlowConfig setCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup checkpointCleanup) {
        if (checkpointCleanup != null) {
            this.checkpointCleanup = checkpointCleanup;
        }
        return this;
    }

    public FlowConfig setStateBackend(StateBackend stateBackend) {
        if (stateBackend != null) {
            this.stateBackend = stateBackend;
        }
        return this;
    }

    public FlowConfig setRestartStrategy(RestartStrategies.FailureRateRestartStrategyConfiguration restartStrategy) {
        if (restartStrategy != null) {
            this.restartStrategy = restartStrategy;
        }
        return this;
    }

    public static FlowConfig mergeFlowConfig(@NonNull String jobName, FlowConfig flowConfig) {
        if (flowConfig == null) {
            return new FlowConfig(jobName);
        }
        val config = new FlowConfig(jobName);
        return config.setCheckpointMode(flowConfig.getCheckpointMode())
                .setCheckpointInterval(flowConfig.getCheckpointInterval())
                .setCheckpointStorage(flowConfig.getCheckpointStorage())
                .setCheckpointTimeout(flowConfig.getCheckpointTimeout())
                .setMinPauseBetweenCheckpoints(flowConfig.getMinPauseBetweenCheckpoints())
                .setMaxConcurrentCheckpoints(flowConfig.getMaxConcurrentCheckpoints())
                .setTolerableCheckpointFailureNumber(flowConfig.getTolerableCheckpointFailureNumber())
                .setEnableUnalignedCheckpoint(flowConfig.isEnableUnalignedCheckpoint())
                .setAlignedCheckpointTimeout(flowConfig.getAlignedCheckpointTimeout())
                .setCheckpointCleanup(flowConfig.getCheckpointCleanup())
                .setStateBackend(flowConfig.getStateBackend())
                .setRestartStrategy(flowConfig.getRestartStrategy());
    }
}
