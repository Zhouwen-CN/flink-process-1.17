package com.yeeiee.apps.sensor;

import com.yeeiee.core.bean.Key;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    /**
     * 水位传感器类型
     */
    @Key
    public String id;
    /**
     * 传感器记录时间戳
     */
    public Long ts;
    /**
     * 传感器记录值
     */
    public Long vc;
}
