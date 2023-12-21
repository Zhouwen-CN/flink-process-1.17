package com.yeeiee.core.bean;

import lombok.val;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

public abstract class AbstractDataGenFunction<IN> implements GeneratorFunction<Long, IN> {
    /**
     * 最大乱序时间
     */
    protected final int maxOutOfOrderSecond;
    protected final Random random;

    protected AbstractDataGenFunction(int maxOutOfOrderSecond) {
        this.maxOutOfOrderSecond = maxOutOfOrderSecond;
        this.random = new Random();
    }

    /**
     * 获取时间戳
     *
     * @return 毫秒
     */
    protected long getTs() {
        val num = maxOutOfOrderSecond / 2;
        val randomNumber = (random.nextInt(num * 2 + 1) - num) * 1000;
        return System.currentTimeMillis() + randomNumber;
    }


}
