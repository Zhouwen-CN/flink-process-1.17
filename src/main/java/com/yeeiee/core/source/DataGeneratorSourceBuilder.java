package com.yeeiee.core.source;

import com.yeeiee.apps.sensor.DataGeneratorFunction;
import com.yeeiee.apps.sensor.WaterSensor;
import com.yeeiee.core.context.Context;
import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class DataGeneratorSourceBuilder implements SourceBuilder<WaterSensor> {
    /**
     * 最大乱序时间
     *
     * @return 秒
     */
    protected abstract int maxOutOfOrderSecond();

    @Override
    public DataStream<WaterSensor> build(Context context) {
        val dataStream = context.getDataStream();
        val generatorSource = new DataGeneratorSource<>(
                new DataGeneratorFunction(maxOutOfOrderSecond()),
                Integer.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                Types.POJO(WaterSensor.class)
        );
        return dataStream
                .fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "data-generator-source")
                .setParallelism(1);
    }
}
