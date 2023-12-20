package com.yeeiee.core.sink;


import com.yeeiee.core.context.Context;
import org.apache.flink.streaming.api.datastream.DataStream;

public class KafkaSinkBuilder<OUT> implements SinkBuilder<OUT> {
    @Override
    public void build(Context context, DataStream<OUT> output) {
    }
}
