package com.yeeiee.core.flow;

import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import org.apache.flink.api.common.RuntimeExecutionMode;

public interface Flow {
    RuntimeExecutionMode runtimeMode();

    void run(Context context) throws BasicException;
}
