package com.yeeiee.apps.jdbc;

import com.yeeiee.apps.sensor.WaterSensor;
import com.yeeiee.core.catalog.CatalogConstant;
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
            public Integer interval() {
                return 1;
            }

            @Override
            public String catalog() {
                return CatalogConstant.jdbcCatalog.getName();
            }

            @Override
            public String databaseWithTableName() {
                return "chen_test.water_sensor1";
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
