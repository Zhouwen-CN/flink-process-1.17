package com.yeeiee.core.source;

import com.yeeiee.core.context.Context;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class KafkaSourceBuilder<IN> implements SourceBuilder<IN> {

    /**
     * @return kafka bootstrap servers
     */
    protected abstract String bootstrapServers();

    /**
     * @return kafka topic
     */
    protected abstract String topic();

    /**
     * @return kafka deserializer
     */
    protected abstract DeserializationSchema<IN> deserializer();

    @Override
    public DataStream<IN> build(Context context) {
        KafkaSource<IN> kafkaSource = KafkaSource.<IN>builder()
                .setBootstrapServers(bootstrapServers())
                .setGroupId(context.getJobName())
                .setTopics(topic())
                .setValueOnlyDeserializer(deserializer())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        return context.getDataStream()
                .fromSource(kafkaSource, watermark(), "kafka-source");
    }


}
