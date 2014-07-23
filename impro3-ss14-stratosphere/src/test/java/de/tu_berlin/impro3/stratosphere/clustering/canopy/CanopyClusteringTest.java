package de.tu_berlin.impro3.stratosphere.clustering.canopy;

import eu.stratosphere.test.util.JavaProgramTestBase;

import java.util.ArrayList;

public class CanopyClusteringTest extends JavaProgramTestBase {

    private static String inputPath;

    private static String outputPath;

    private static float t1;

    private static float t2;

    private static int maxIterations;

    @Override
    protected void preSubmit() throws Exception {
        inputPath = this.getClass().getResource("/datasets/clustering/bag-of-words/docword.kos.txt").toURI().toString();
        outputPath = getTempDirPath("canopy-output");
        t1 = 0.06f;
        t2 = 0.1f;
        maxIterations = Integer.MAX_VALUE;
    }

    @Override
    protected void testProgram() throws Exception {
        testKosData();
    }

    public void testKosData() throws Exception {
        // run canopy
        new Canopy(inputPath, outputPath, t1, t2, maxIterations).run();

        ArrayList<String> list = new ArrayList<>();
        readAllResultLines(list, outputPath, false);
        System.out.println("Canopy clustering ran fine. You may run k-means inside the clusters which look like:");
        System.out.println("...");
        for (String line : list.subList(5, 10)) {
            System.out.println(line);
        }
        System.out.println("...");
    }
}
