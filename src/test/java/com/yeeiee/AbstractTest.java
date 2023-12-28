package com.yeeiee;

import com.yeeiee.apps.sensor.WaterSensor;
import com.yeeiee.apps.sensor.WaterSensorDataGenFunction;
import com.yeeiee.core.bean.AbstractDataGenFunction;
import com.yeeiee.core.context.Context;
import com.yeeiee.core.source.DataGenSourceBuilder;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;

@Slf4j
public abstract class AbstractTest extends TestCase {
    protected Context context;
    protected DataStream<WaterSensor> dataGenSource;

    public AbstractTest() {
        val simpleName = this.getClass().getSimpleName();
        super.setName(simpleName);
    }

    @Override
    protected void setUp() {
        context = Context.builder()
                .setJobClass(this.getClass())
                .setRuntimeMode(RuntimeExecutionMode.STREAMING)
                .setParallelism(1)
                .build(null);
        dataGenSource = new DataGenSourceBuilder<WaterSensor>() {
            @Override
            protected AbstractDataGenFunction<WaterSensor> generatorFunction() {
                return new WaterSensorDataGenFunction(5);
            }
        }.build(context);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
