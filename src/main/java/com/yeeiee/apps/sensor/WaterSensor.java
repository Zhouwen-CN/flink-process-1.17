package com.yeeiee.apps.sensor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 测试使用
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    // 水位传感器类型
    public String id;
    // 传感器记录时间戳
    public Long ts;
    // 传感器记录值
    public Long vc;
}
