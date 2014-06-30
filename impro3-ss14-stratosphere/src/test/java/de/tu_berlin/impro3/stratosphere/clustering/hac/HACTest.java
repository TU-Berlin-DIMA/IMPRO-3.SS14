package de.tu_berlin.impro3.stratosphere.clustering.hac;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;

import de.tu_berlin.impro3.stratosphere.clustering.hac.HAC.LinkageMode;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.test.util.JavaProgramTestBase;

public class HACTest extends JavaProgramTestBase {

    private List<HAC.ClusterPair> generateSimilarities() {
        List<HAC.ClusterPair> result = new ArrayList<>();

        result.add(new HAC.ClusterPair(10.0, 0, 1));
        result.add(new HAC.ClusterPair(15.0, 0, 2));
        result.add(new HAC.ClusterPair(12.0, 0, 3));
        result.add(new HAC.ClusterPair(13.0, 1, 2));
        result.add(new HAC.ClusterPair(14.0, 1, 3));
        result.add(new HAC.ClusterPair(16.0, 2, 3));

        return result;
    }

    private void updateOldIds(List<HAC.ClusterPair> list, int oldid, int newid) {
        for (HAC.ClusterPair pair : list) {
            if (pair.getCluster1().equals(oldid))
                pair.setCluster1(newid);
            else if (pair.getCluster2().equals(oldid))
                pair.setCluster2(newid);
        }
    }

    private List<HAC.ClusterPair> removeSameCluster(List<HAC.ClusterPair> list) {
        ArrayList<HAC.ClusterPair> result = new ArrayList<>();

        for (HAC.ClusterPair pair : list) {
            if (!pair.getCluster1().equals(pair.getCluster2()))
                result.add(pair);
        }

        return result;
    }

    private String generateResultString(List<HAC.ClusterPair> data, final boolean min) {
        Collections.sort(data, new Comparator<HAC.ClusterPair>() {

            @Override
            public int compare(HAC.ClusterPair o1, HAC.ClusterPair o2) {
                if (o1.getSimilarity() < o2.getSimilarity())
                    return min ? -1 : 1;
                else
                    return min ? 1 : -1;
            }
        });

        HAC.ClusterHistory result = new HAC.ClusterHistory();

        while (data.size() > 1) {
            HAC.ClusterPair pair = data.get(0);
            result.add(new HAC.ClusterHistory(pair.getCluster1(), pair.getCluster2()));
            updateOldIds(data, pair.getCluster1(), pair.getCluster2());
            data = removeSameCluster(data);
        }

        return result.toString();
    }

    @Override
    protected void testProgram() throws Exception {
        String outputPath = getTempDirPath("result");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<HAC.ClusterPair> similarities = generateSimilarities();
        DataSet<HAC.ClusterPair> sim = env.fromCollection(similarities);
        HAC.clusterDocuments(env, LinkageMode.SINGLE, sim, outputPath, 100);

        long startTime = System.currentTimeMillis();
        env.execute();
        System.out.println("Execution Time: " + (System.currentTimeMillis() - startTime));

        List<String> resultLines = new ArrayList<String>(1);
        readAllResultLines(resultLines, outputPath);

        String expectedResult = generateResultString(similarities, false);

        Assert.assertEquals("Output line count", 1, resultLines.size());
        Assert.assertEquals("Cluster history", expectedResult, resultLines.get(0));
    }
}
