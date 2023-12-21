package com.yeeiee.core.flow;

import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import org.apache.flink.api.common.RuntimeExecutionMode;

public interface Flow {
    /**
     * flow并行度
     *
     * @return default 1
     */
    default int parallelism() {
        return 1;
    }

    /**
     * flow 执行模式
     *
     * @return default streaming
     */
    default RuntimeExecutionMode runtimeMode() {
        return RuntimeExecutionMode.STREAMING;
    }

    void run(Context context) throws BasicException;
}
