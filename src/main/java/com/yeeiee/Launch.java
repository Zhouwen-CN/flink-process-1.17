package com.yeeiee;

import com.yeeiee.core.context.Context;
import com.yeeiee.core.flow.Flow;
import com.yeeiee.exception.BasicException;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.flink.api.java.utils.ParameterTool;

@Slf4j
public class Launch {
    public static void main(String[] args) throws BasicException {
        val parameterTool = ParameterTool.fromArgs(args);
        if (!parameterTool.has("main")) {
            throw new BasicException("Could not find the --main parameter");
        }
        val classFullName = parameterTool.get("main");
        try {
            val clazz = Class.forName(classFullName);
            val flow = (Flow) clazz.newInstance();
            val runtimeMode = flow.runtimeMode();
            int parallelism = flow.parallelism();
            val context = Context.builder()
                    .setJobClass(clazz)
                    .setRuntimeMode(runtimeMode)
                    .setParallelism(parallelism)
                    .build();
            flow.run(context);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new BasicException(e.getMessage());
        }
    }
}
