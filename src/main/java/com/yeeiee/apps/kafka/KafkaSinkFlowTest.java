package com.yeeiee.apps.kafka;

import com.yeeiee.apps.sensor.WaterSensor;
import com.yeeiee.core.flow.AbstractSingleFlow;
import com.yeeiee.core.sink.KafkaSinkBuilder;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.DataGeneratorSourceBuilder;
import com.yeeiee.core.source.SourceBuilder;
import com.yeeiee.utils.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;

public class KafkaSinkFlowTest extends AbstractSingleFlow<WaterSensor, String> {

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
    public SinkBuilder<String> sink() {
        return new KafkaSinkBuilder<String>() {
            @Override
            protected String bootstrapServers() {
                return "192.168.135.110:9092";
            }

            @Override
            protected String topics() {
                return "test";
            }

            @Override
            protected SerializationSchema<String> serializer() {
                return new SimpleStringSchema();
            }

            @Override
            protected DeliveryGuarantee deliveryGuarantee() {
                return DeliveryGuarantee.EXACTLY_ONCE;
            }
        };
    }

    @Override
    public DataStream<String> transform(DataStream<WaterSensor> input) {
        return input.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) throws Exception {
                return JsonUtil.toJsonString(waterSensor);
            }
        });
    }
}
