package com.yeeiee.apps.sensor;

import com.yeeiee.core.flow.AbstractSingleFlow;
import com.yeeiee.core.sink.ConsolePrintSink;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.DataGeneratorSourceBuilder;
import com.yeeiee.core.source.SourceBuilder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;

public class WaterSensorFlow extends AbstractSingleFlow<WaterSensor, WaterSensor> {
    @Override
    public RuntimeExecutionMode runtimeMode() {
        return RuntimeExecutionMode.STREAMING;
    }

    @Override
    protected SourceBuilder<WaterSensor> source() {
        return new DataGeneratorSourceBuilder() {
            @Override
            protected int maxOutOfOrderSecond() {
                return 5;
            }
        };
    }

    @Override
    public SinkBuilder<WaterSensor> sink() {
        return new ConsolePrintSink<WaterSensor>();
    }

    @Override
    public DataStream<WaterSensor> transform(DataStream<WaterSensor> input) {
        return input;
    }
}
