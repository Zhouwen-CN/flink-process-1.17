package com.yeeiee.core.source;

import com.yeeiee.core.context.Context;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface SourceBuilder<IN> {
    DataStream<IN> build(Context context);
}
