package com.yeeiee.apps.sensor;

import lombok.val;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

public class DataGeneratorFunction implements GeneratorFunction<Long, WaterSensor> {
        private final Random random ;
        private final int maxOutOfOrderSecond;
        private final String[] ids;

        public DataGeneratorFunction(int maxOutOfOrderSecond, String[] ids){
            this.maxOutOfOrderSecond = maxOutOfOrderSecond;
            this.ids = ids;
            this.random =  new Random();
        }

        private long getTs() {
            val randomNumber = (random.nextInt(maxOutOfOrderSecond * 2 + 1) - maxOutOfOrderSecond) * 1000;
            return System.currentTimeMillis() + randomNumber;
        }

        private String getId(){
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