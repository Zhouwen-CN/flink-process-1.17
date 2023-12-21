package com.yeeiee.core.source;

import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import com.yeeiee.utils.ReflectUtil;
import lombok.val;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class DataGenSourceBuilder<IN> implements SourceBuilder<IN> {
    /**
     * @return data generator function
     */
    protected abstract GeneratorFunction<Long, IN> generatorFunction();

    @SuppressWarnings("unchecked")
    @Override
    public DataStream<IN> build(Context context) throws BasicException {
        val dataStream = context.getDataStream();
        val superClassT = (Class<IN>) ReflectUtil.getSuperClassT(this);
        val generatorSource = new DataGeneratorSource<>(
                generatorFunction(),
                Integer.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                Types.POJO(superClassT)
        );
        return dataStream
                .fromSource(generatorSource, watermark(), "data-generator-source");
    }
}
