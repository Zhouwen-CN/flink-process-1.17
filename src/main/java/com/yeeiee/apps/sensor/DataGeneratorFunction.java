package com.yeeiee.apps.sensor;

import lombok.val;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

public class DataGeneratorFunction implements GeneratorFunction<Long, WaterSensor> {
    private final Random random;
    private final int maxOutOfOrderSecond;
    // mock id list
    public static final String[] ids = new String[]{"北京", "上海", "广州", "深圳"};

    public DataGeneratorFunction(int maxOutOfOrderSecond) {
        this.maxOutOfOrderSecond = maxOutOfOrderSecond;
        this.random = new Random();
    }

    private long getTs() {
        val num = maxOutOfOrderSecond / 2;
        val randomNumber = (random.nextInt(num * 2 + 1) - num) * 1000;
        return System.currentTimeMillis() + randomNumber;
    }

    private String getId() {
        val index = random.nextInt(ids.length);
        return ids[index];
    }

    @Override
    public WaterSensor map(Long aLong) throws Exception {
        val id = getId();
        val ts = getTs();
        return new WaterSensor(id, ts, aLong);
    }
}