package com.yeeiee.apps.jdbc;

import com.yeeiee.apps.sensor.WaterSensor;
import com.yeeiee.core.bean.JdbcOptions;
import com.yeeiee.core.flow.AbstractSingleFlow;
import com.yeeiee.core.sink.ConsolePrintSink;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.CustomJdbcSourceBuilder;
import com.yeeiee.core.source.SourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

public class JdbcSourceFlowTest extends AbstractSingleFlow<WaterSensor, WaterSensor> {
    @Override
    protected SourceBuilder<WaterSensor> source() {
        return new CustomJdbcSourceBuilder<WaterSensor>() {
            @Override
            public JdbcOptions jdbcOptions() {
                return new JdbcOptions()
                        .setDriverName("com.mysql.cj.jdbc.Driver")
                        .setUrl("jdbc:mysql://127.0.0.1:3306/chen_test")
                        .setUsername("root")
                        .setPassword("123")
                        .setTableName("water_sensor");
            }

            @Override
            public Integer interval() {
                return 1;
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
