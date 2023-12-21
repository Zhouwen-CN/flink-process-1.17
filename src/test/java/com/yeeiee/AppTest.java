package com.yeeiee;

import com.yeeiee.core.context.Context;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;

/**
 * Unit test for simple App.
 */
@Slf4j
public class AppTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    private Context context;

    @Override
    protected void setUp() {
        context = Context
                .builder()
                .setJobClass(AppTest.class)
                .setParallelism(1)
                .setRuntimeMode(RuntimeExecutionMode.STREAMING)
                .build(null);
    }

    public void testLog() {
        for (int i = 0; i < 50; i++) {
            log.info("info log");
        }
        for (int i = 0; i < 50; i++) {
            log.warn("warn log");
        }
        for (int i = 0; i < 50; i++) {
            log.error("error log");
        }
    }

}
