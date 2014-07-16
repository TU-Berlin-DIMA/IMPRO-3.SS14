package de.tu_berlin.impro3.spark.classification.logreg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class LogisticRegressionTests {

    private JavaSparkContext sc;

    private File resultPath;

    private String resourcePath;

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "LogisticRegressionTests");
        resultPath = Files.createTempDir();
        resultPath.deleteOnExit();
        File resultPath2 = Files.createTempDir();
        resultPath2.deleteOnExit();

        resourcePath = getClass().getResource("../../../../../..").getFile();
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    /*
     * The first three tests test whether or not three predefined datasets produce the correct theta
     * value. The correctness of the theta value was validated through a scala and a Matlab
     * implementation of the logistic regression algorithm.
     */

    @Test
    public void testProgram1() throws Exception {
        int numberOfFeatures = 1;
        String pointsWithLabelsPath = resourcePath + "cub_gen_output_10";
        float alpha = 1;
        int maxIterations = 5000;
        String outputPathTheta = resultPath.getAbsolutePath();
        int labelPosition = 2;

        new LogisticRegression(sc, numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition).run();

        ArrayList<String> list = new ArrayList<String>();
        readAllResultLines(list, resultPath, false);

        double result = Double.parseDouble(list.get(0).replaceAll("[^0-9.,-]+", ""));

        Assert.assertTrue(result > 6.0 && result < 6.2);
    }

    @Test
    public void testProgram2() throws Exception {
        int numberOfFeatures = 1;
        String pointsWithLabelsPath = resourcePath + "cub_gen_output_100";
        float alpha = 1;
        int maxIterations = 5000;
        String outputPathTheta = resultPath.getAbsolutePath();
        int labelPosition = 2;

        new LogisticRegression(sc, numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition).run();

        ArrayList<String> list = new ArrayList<String>();
        readAllResultLines(list, resultPath, false);

        double result = Double.parseDouble(list.get(0).replaceAll("[^0-9.,-]+", ""));

        Assert.assertTrue(result > 1.2 && result < 1.4);
    }

    @Test
    public void testProgram3() throws Exception {
        int numberOfFeatures = 1;
        String pointsWithLabelsPath = resourcePath + "cub_gen_output_1000";
        float alpha = 1;
        int maxIterations = 5000;
        String outputPathTheta = resultPath.getAbsolutePath();
        int labelPosition = 2;

        new LogisticRegression(sc, numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition).run();

        ArrayList<String> list = new ArrayList<String>();
        readAllResultLines(list, resultPath, false);

        double result = Double.parseDouble(list.get(0).replaceAll("[^0-9.,-]+", ""));

        Assert.assertTrue(result > 1.6 && result < 1.8);
    }

    // --------------------------------------------------------------------------------------------
    // Result Checking
    // Source: Stratosphere Project (http://stratosphere.eu)
    // File: eu.stratosphere.test.util.AbstractTestBase
    // --------------------------------------------------------------------------------------------

    public void readAllResultLines(List<String> target, File resultPath, boolean inOrderOfFiles) throws IOException {
        for (BufferedReader reader : getResultReader(resultPath, inOrderOfFiles)) {
            String s;
            while ((s = reader.readLine()) != null) {
                target.add(s);
            }
        }
    }

    public BufferedReader[] getResultReader(File resultPath, boolean inOrderOfFiles) throws IOException {
        File[] files = getAllInvolvedFiles(resultPath);

        if (inOrderOfFiles) {
            // sort the files after their name (1, 2, 3, 4)...
            // we cannot sort by path, because strings sort by prefix
            Arrays.sort(files, new Comparator<File>() {

                @Override
                public int compare(File o1, File o2) {
                    try {
                        int f1 = Integer.parseInt(o1.getName());
                        int f2 = Integer.parseInt(o2.getName());
                        return f1 < f2 ? -1 : (f1 > f2 ? 1 : 0);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("The file names are no numbers and cannot be ordered: " + o1.getName() + "/" + o2.getName());
                    }
                }
            });
        }

        BufferedReader[] readers = new BufferedReader[files.length];
        for (int i = 0; i < files.length; i++) {
            readers[i] = new BufferedReader(new FileReader(files[i]));
        }
        return readers;
    }

    private File[] getAllInvolvedFiles(File result) {
        if (!result.exists()) {
            Assert.fail("Result file was not written");
        }
        if (result.isDirectory()) {
            return result.listFiles();
        } else {
            return new File[] {result};
        }
    }

}
