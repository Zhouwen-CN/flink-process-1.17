package com.yeeiee.apps.sensor;

import com.yeeiee.core.bean.AbstractDataGenFunction;
import lombok.val;

public class WaterSensorDataGenFunction extends AbstractDataGenFunction<WaterSensor> {
    /**
     * mock id list
     */
    private static final String[] IDS = new String[]{"北京", "上海", "广州", "深圳"};

    public WaterSensorDataGenFunction(int maxOutOfOrderSecond) {
        super(maxOutOfOrderSecond);
    }

    private String getId() {
        val index = random.nextInt(IDS.length);
        return IDS[index];
    }

    @Override
    public WaterSensor map(Long aLong) {
        val id = getId();
        val ts = super.getTs();
        return new WaterSensor(id, ts, aLong);
    }
}