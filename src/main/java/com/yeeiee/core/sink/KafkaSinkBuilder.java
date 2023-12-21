package com.yeeiee.core.sink;


import com.yeeiee.core.context.Context;
import lombok.val;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kafka.clients.producer.ProducerConfig;

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
     * @return 传输保证, 精准一次|至少一次
     */
    protected abstract DeliveryGuarantee deliveryGuarantee();

    @Override
    public void build(Context context, DataStream<OUT> output) {
        val deliveryGuarantee = deliveryGuarantee();
        org.apache.flink.connector.kafka.sink.KafkaSinkBuilder<OUT> kafkaSinkBuilder = KafkaSink.<OUT>builder()
                .setBootstrapServers(bootstrapServers())
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<OUT>builder()
                                .setTopic(topic())
                                .setValueSerializationSchema(serializer())
                                .build()
                )
                .setDeliveryGuarantee(deliveryGuarantee);

        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            kafkaSinkBuilder
                    // 如果是精准一次，必须设置 事务的前缀
                    .setTransactionalIdPrefix(context.getJobName() + "-")
                    // 如果是精准一次，必须设置 事务超时时间: 大于checkpoint间隔，小于 max 15分钟
                    .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "");
        }
        output.sinkTo(kafkaSinkBuilder.build());
    }
}
