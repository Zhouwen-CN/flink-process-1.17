package com.yeeiee.apps.kafka;

import com.yeeiee.core.flow.AbstractSingleFlow;
import com.yeeiee.core.sink.ConsolePrintSink;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.KafkaSourceBuilder;
import com.yeeiee.core.source.SourceBuilder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;

public class KafkaFlowTest extends AbstractSingleFlow<String, String> {
    @Override
    protected SourceBuilder<String> source() {
        return new KafkaSourceBuilder<String>() {
            @Override
            protected String bootstrapServers() {
                return "192.168.135.110:9092";
            }

            @Override
            protected String topics() {
                return "test";
            }

            @Override
            protected DeserializationSchema<String> deserializer() {
                return new SimpleStringSchema();
            }
        };
    }

    @Override
    public SinkBuilder<String> sink() {
        return new ConsolePrintSink<>();
    }

    @Override
    public DataStream<String> transform(DataStream<String> input) {
        return input;
    }

    @Override
    public RuntimeExecutionMode runtimeMode() {
        return RuntimeExecutionMode.STREAMING;
    }
}