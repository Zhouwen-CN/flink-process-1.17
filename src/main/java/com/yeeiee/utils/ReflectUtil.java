package com.yeeiee.utils;

import com.yeeiee.core.bean.Key;
import com.yeeiee.exception.BasicException;
import lombok.experimental.UtilityClass;
import lombok.val;
import lombok.var;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        throw new BasicException("Cannot get the super class generic");
    }

    /**
     * 反射获取 主键名列表 和 字段名列表
     *
     * @param clazz Class 对象
     * @return tuple2
     */
    public Tuple2<List<String>, List<String>> getKeyFieldNames(Class<?> clazz) {
        val keyNames = new ArrayList<String>();
        val fieldNames = new ArrayList<String>();

        Arrays.stream(clazz.getDeclaredFields()).forEach(f -> {
            f.setAccessible(true);
            val fieldName = f.getName();
            if (f.getAnnotation(Key.class) != null) {
                keyNames.add(fieldName);
            }
            fieldNames.add(fieldName);
        });

        return Tuple2.of(keyNames, fieldNames);
    }

    /**
     * 检查类中是否含有某个方法
     *
     * @param clazz      类
     * @param methodName 方法名称
     * @return boolean
     */
    public boolean checkMethodExists(Class<?> clazz, String methodName) {
        val count = Arrays.stream(clazz.getMethods())
                .map(Method::getName)
                .filter(methodName::equals)
                .count();
        return count > 0;
    }

    /**
     * 检查类中所有的字段是否都有set方法
     *
     * @param clazz 类
     * @return boolean
     */
    public boolean checkBeanHasSetMethods(Class<?> clazz) {
        val methodNames = Arrays.stream(clazz.getMethods())
                .map(Method::getName)
                .collect(Collectors.toList());
        return Arrays.stream(clazz.getDeclaredFields())
                .map(f -> "set" + StringUtils.capitalize(f.getName()))
                .allMatch(methodNames::contains);

    }
}
