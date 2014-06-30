package de.tu_berlin.impro3.stratosphere.clustering.hac;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import junit.framework.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;

public class HAC_FlinkJavaTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private void testCompleteLinkage(ArrayList<HAC_Flink_Java.ClusterPair> sim) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<HAC_Flink_Java.ClusterPair> similarities = env.fromCollection(sim);
        HAC_Flink_Java.clusterDocuments(env, "COMPLETE", similarities, testFolder.getRoot() + "/completetest");

        env.execute();
    }

    @Test
    public void testclusterDocuments() throws Exception {
	    TemporaryFolder testFolder = new TemporaryFolder();
	    File inputFile = new File("./impro3-ss14-stratosphere/src/main/resources/datasets/clustering/bag-of-words/docword.kos.txt.mini");
	    testFolder.create();
	    String[] args = {inputFile.getCanonicalPath(), "SINGLE", testFolder.getRoot().getPath(), "false"};
		HAC_Flink_Java.main(args);

	    Scanner scanner = new Scanner(new File(testFolder.getRoot().getPath()));
	    String line = scanner.nextLine();
	    if (!line.equals("[4; 5][1; 5][5; 6][3; 6][2; 6]")) {
		    Assert.fail("Incorrect merging by HAC");
	    }
    }
}