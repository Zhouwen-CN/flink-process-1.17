package com.yeeiee.core.sink;


import com.yeeiee.core.context.Context;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kafka.clients.producer.ProducerConfig;

@Slf4j
public abstract class KafkaSinkBuilder<OUT> implements SinkBuilder<OUT> {
    /**
     * @return kafka bootstrap servers
     */
    protected abstract String bootstrapServers();

    /**
     * @return kafka topic
     */
    protected abstract String topic();

    /**
     * @return kafka serializer
     */

    protected abstract SerializationSchema<OUT> serializer();

    /**
     * 传输保证, 精准一次|至少一次
     *
     * @return default AT_LEAST_ONCE
     */
    protected abstract DeliveryGuarantee deliveryGuarantee();

    @Override
    public void build(Context context, DataStream<OUT> output) {
        val bootstrapServers = bootstrapServers();
        val topic = topic();
        val serializer = serializer();
        var deliveryGuarantee = deliveryGuarantee();

        // 只有当传输保证为精准一次 且同时 检查点为精准一次,才能开启kafka sink 精准一次
        if (deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE ||
                context.getFlowConfig().getCheckpointMode() != CheckpointingMode.EXACTLY_ONCE) {
            deliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE;
        }
        val kafkaSinkBuilder = KafkaSink.<OUT>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<OUT>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(serializer)
                                .build()
                )
                .setDeliveryGuarantee(deliveryGuarantee);

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            kafkaSinkBuilder
                    // 如果是精准一次,必须设置 事务的前缀
                    .setTransactionalIdPrefix(context.getJobName() + "-")
                    // 如果是精准一次,必须设置 事务超时时间: 大于checkpoint间隔,小于 max 15分钟
                    .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "");
        }

        log.info("Use KafkaSink config: bootstrapServers={}, topic={}, serializer={}, deliveryGuarantee={}", bootstrapServers, topic, serializer, deliveryGuarantee);
        output.sinkTo(kafkaSinkBuilder.build());
    }
}
