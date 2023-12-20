package com.yeeiee;

import com.yeeiee.core.context.Context;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.Random;

/**
 * Unit test for simple App.
 */
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

    /**
     * Rigourous Test :-)
     */
    public void testCreateContext() {
        Context build = Context.builder()
                .setJobClass(AppTest.class)
                .build();

        assertNotNull(build);
    }

    public void testRandom(){
        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            System.out.println(random.nextInt(5));
        }

    }
}
