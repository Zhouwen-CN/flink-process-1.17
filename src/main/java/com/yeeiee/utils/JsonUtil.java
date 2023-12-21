package com.yeeiee.utils;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonUtil {
    private final JsonMapper jsonMapper = JsonMapper.builder()
            // 反序列遇到不存在的java bean属性, 不会报错
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .build();

    /**
     * 将java对象转换为json字符串
     *
     * @param obj 需要转换的java对象
     * @return json 字符串
     * @throws JsonProcessingException parse failed
     */
    public String toJsonString(Object obj) throws JsonProcessingException {
        return jsonMapper.writeValueAsString(obj);
    }
}
