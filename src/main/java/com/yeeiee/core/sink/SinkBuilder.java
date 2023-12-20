package com.yeeiee.core.sink;

import com.yeeiee.core.context.Context;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface SinkBuilder<OUT> {
    void build(Context context,DataStream<OUT> output);
}
