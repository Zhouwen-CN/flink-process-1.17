package com.yeeiee.apps.sensor;

import com.yeeiee.core.flow.AbstractSingleFlow;
import com.yeeiee.core.sink.ConsolePrintSink;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.DataGeneratorSourceBuilder;
import com.yeeiee.core.source.SourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

public class WaterSensorFlow extends AbstractSingleFlow<WaterSensor, WaterSensor> {

    @Override
    protected SourceBuilder<WaterSensor> source() {
        return new DataGeneratorSourceBuilder<WaterSensor>() {
            @Override
            protected int maxOutOfOrderSecond() {
                return 5;
            }
        };
    }

    @Override
    public SinkBuilder<WaterSensor> sink() {
        return new ConsolePrintSink<>();
    }

    @Override
    public DataStream<WaterSensor> transform(DataStream<WaterSensor> input) {
        return input;
    }
}
