package com.yeeiee.apps.kafka;

import com.yeeiee.apps.sensor.WaterSensorDataGenFunction;
import com.yeeiee.apps.sensor.WaterSensor;
import com.yeeiee.core.flow.AbstractSingleFlow;
import com.yeeiee.core.sink.KafkaSinkBuilder;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.DataGenSourceBuilder;
import com.yeeiee.core.source.SourceBuilder;
import com.yeeiee.utils.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class KafkaSinkFlowTest extends AbstractSingleFlow<WaterSensor, String> {

    @Override
    protected SourceBuilder<WaterSensor> source() {
        return new DataGenSourceBuilder<WaterSensor>() {

            @Override
            protected GeneratorFunction<Long, WaterSensor> generatorFunction() {
                return new WaterSensorDataGenFunction(5);
            }
        };
    }

    @Override
    public SinkBuilder<String> sink() {
        return new KafkaSinkBuilder<String>() {
            @Override
            protected String bootstrapServers() {
                return "127.0.0.1:9092";
            }

            @Override
            protected String topic() {
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
        return input.map((MapFunction<WaterSensor, String>) JsonUtil::toJsonString);
    }
}
