package com.yeeiee.utils;

import com.yeeiee.exception.BasicException;
import lombok.experimental.UtilityClass;
import lombok.val;
import lombok.var;

import java.lang.reflect.ParameterizedType;

@UtilityClass
public class ReflectUtil {
    /**
     * 获取父类的泛型
     *
     * @param obj 当前类
     * @return 默认返回第一个
     * @throws BasicException not found
     */
    public Class<?> getSuperClassT(Object obj) throws BasicException {
        var type = obj.getClass().getGenericSuperclass();

        if (type instanceof ParameterizedType) {
            val parameterizedType = (ParameterizedType) type;
            type = parameterizedType.getActualTypeArguments()[0];
            if (type instanceof Class<?>) {
                return (Class<?>) type;
            }
        }
        throw new BasicException("cannot get the super class generic ...");
    }
}
