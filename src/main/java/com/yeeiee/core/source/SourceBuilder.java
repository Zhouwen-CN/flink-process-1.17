package com.yeeiee.core.source;

import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface SourceBuilder<IN> {
    /**
     * flink 水印
     *
     * @return default none
     */
    default WatermarkStrategy<IN> watermark() {
        return WatermarkStrategy.noWatermarks();
    }

    DataStream<IN> build(Context context) throws BasicException;
}
