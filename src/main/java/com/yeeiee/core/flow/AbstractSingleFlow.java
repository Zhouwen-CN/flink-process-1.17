package com.yeeiee.core.flow;

import com.yeeiee.core.context.Context;
import com.yeeiee.core.sink.SinkBuilder;
import com.yeeiee.core.source.SourceBuilder;
import com.yeeiee.exception.BasicException;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class AbstractSingleFlow<IN, OUT> extends AbstractFlow {
    protected abstract SourceBuilder<IN> source();

    public abstract SinkBuilder<OUT> sink();

    public abstract DataStream<OUT> transform(DataStream<IN> input);

    @SneakyThrows(BasicException.class)
    @Override
    public void run(Context context) {
        val input = source().build(context);
        val transform = transform(input);
        sink().build(context, transform);

        super.submit(context);
    }
}
