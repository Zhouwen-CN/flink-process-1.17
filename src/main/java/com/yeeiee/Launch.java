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
        if (!parameterTool.has("flow")) {
            throw new BasicException("Could not find the --flow parameter");
        }
        val classFullName = parameterTool.get("flow");
        try {
            val clazz = Class.forName(classFullName);
            val flow = (Flow) clazz.newInstance();
            val runtimeMode = flow.runtimeMode();
            val parallelism = flow.parallelism();
            val flowConfig = flow.flowConfig();
            val context = Context.builder()
                    .setJobClass(clazz)
                    .setRuntimeMode(runtimeMode)
                    .setParallelism(parallelism)
                    .build(flowConfig);
            flow.run(context);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new BasicException(e.getMessage());
        }
    }
}
