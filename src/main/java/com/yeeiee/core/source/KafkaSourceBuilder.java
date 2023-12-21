package com.yeeiee.core.source;

import com.yeeiee.core.context.Context;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;

import java.util.Locale;

@Slf4j
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
    /**
     * @return 隔离级别
     */
    protected  abstract IsolationLevel isolationLevel();

    @Override
    public DataStream<IN> build(Context context) {
        val bootstrapServers = bootstrapServers();
        val topic = topic();
        val deserializer = deserializer();
        val isolationLevel = isolationLevel().toString().toLowerCase(Locale.ROOT);
        val kafkaSource = KafkaSource.<IN>builder()
                .setBootstrapServers(bootstrapServers)
                .setGroupId(context.getJobName())
                .setTopics(topic)
                .setValueOnlyDeserializer(deserializer)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel)
                .build();

        log.info("Use KafkaSource config: bootstrapServers={}, topic={}, deserializer={}", bootstrapServers, topic, deserializer);
        return context.getDataStream()
                .fromSource(kafkaSource, watermark(), "kafka-source");
    }
}
