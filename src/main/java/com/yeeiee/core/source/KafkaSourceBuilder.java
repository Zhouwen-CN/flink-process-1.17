package com.yeeiee.core.source;

import com.yeeiee.core.context.Context;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class KafkaSourceBuilder<IN> implements SourceBuilder<IN> {

    protected abstract String bootstrapServers();

    protected abstract String topics();

    protected abstract DeserializationSchema<IN> deserializer();


    @Override
    public DataStream<IN> build(Context context) {
        KafkaSource<IN> kafkaSource = KafkaSource.<IN>builder()
                .setBootstrapServers(bootstrapServers())
                .setGroupId(context.getJobName())
                .setTopics(topics())
                .setValueOnlyDeserializer(deserializer())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        return context.getDataStream()
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
    }
}
