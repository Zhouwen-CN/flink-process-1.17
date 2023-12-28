package com.yeeiee.apps.jdbc;

import com.yeeiee.apps.sensor.WaterSensor;
import com.yeeiee.apps.sensor.WaterSensorDataGenFunction;
import com.yeeiee.core.bean.AbstractDataGenFunction;
import com.yeeiee.core.bean.JdbcOptions;
import com.yeeiee.core.flow.AbstractSingleFlow;
import com.yeeiee.core.sink.JdbcSinkBuilder;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.DataGenSourceBuilder;
import com.yeeiee.core.source.SourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

public class JdbcSinkFlowTest extends AbstractSingleFlow<WaterSensor, WaterSensor> {
    @Override
    protected SourceBuilder<WaterSensor> source() {
        return new DataGenSourceBuilder<WaterSensor>() {
            @Override
            protected AbstractDataGenFunction<WaterSensor> generatorFunction() {
                return new WaterSensorDataGenFunction(5);
            }
        };
    }

    @Override
    public SinkBuilder<WaterSensor> sink() {
        return new JdbcSinkBuilder<WaterSensor>() {
            @Override
            public JdbcOptions jdbcOptions() {
                return new JdbcOptions()
                        .setDriverName("com.mysql.cj.jdbc.Driver")
                        .setUrl("jdbc:mysql://127.0.0.1:3306/chen_test")
                        .setUsername("root")
                        .setPassword("123")
                        .setTableName("water_sensor1");
            }
        };
    }

    @Override
    public DataStream<WaterSensor> transform(DataStream<WaterSensor> input) {
        return input;
    }
}
