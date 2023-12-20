package com.yeeiee.utils;

import com.yeeiee.exception.BasicException;
import lombok.experimental.UtilityClass;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

@UtilityClass
public class ReflectUtil {
    /**
     * 获取父类的泛型
     *
     * @param obj 获取父类的泛型
     * @return 返回父类的泛型, 默认返回第一个
     * @throws BasicException
     */
    public Class<?> getSuperClassT(Object obj) throws BasicException {
        Type type = obj.getClass().getGenericSuperclass();

        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            type = parameterizedType.getActualTypeArguments()[0];
            if (type instanceof Class<?>) {
                return (Class<?>) type;
            }
        }

        throw new BasicException("cannot get the super class generic ...");
    }
}
