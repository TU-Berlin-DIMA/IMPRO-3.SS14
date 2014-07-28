package de.tu_berlin.impro3.spark.clustering.kmeanspp;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class KMeansppBagOfWordsTests {

    public static final long seed = 14839204302832L;

    public static final int numPointsPerCluster = 300;

    public static final int numDimensions = 200;

    public static final int k = 5;

    public static final int numIterations = 10;

    public static final int maxClusterRadius = 200;

    public static final int maxFeatureValue = 10000;

    private static final String dataFileName = "testdata";

    public static final String fieldDelimiter = " ";

    private String outputDir = "";

    private static final String resultFileName = File.separator + "part-00000";

    Random random = new Random(seed);

    private JavaSparkContext sc;

    private File resultPath;

    private String resourcePath;

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    List<Cluster> originClusters = new ArrayList<Cluster>(k);

    @Before
    public void generateTestData() throws IOException {
        sc = new JavaSparkContext("local", "TestBOWKmeanspp");

        BufferedWriter out = new BufferedWriter(new FileWriter(new File(tmpFolder.getRoot(), dataFileName)));

        outputDir = new File(tmpFolder.getRoot(), "KmeansppResult").getAbsolutePath();

        // generate initial data set
        for (int c = 0; c < k; ++c) {
            // pick a new center that does not intersect with existing centers
            Cluster cluster = new Cluster();
            cluster.radius = (double) random.nextInt(maxClusterRadius) + 1;
            do {
                for (int w = 0; w < numDimensions; ++w) {
                    cluster.center.words.put(w, (double) random.nextInt(maxFeatureValue));
                }
            } while (intersectWithExistingCenter(cluster, originClusters));

            // add points to the new center
            for (int i = 0; i < numPointsPerCluster; ++i) {
                int docId = i + numPointsPerCluster * c;
                DocPoint p = new DocPoint(docId);
                for (int w = 0; w < numDimensions; ++w) {
                    // generate a point whose word counts are within the range cluster.center +/-
                    // radius
                    int freq = (int) (random.nextInt((int) (cluster.radius * 2)) + cluster.center.words.get(w) - cluster.radius);
                    p.words.put(w, freq > 0 ? (double) freq : 0);
                }
                cluster.pointIds.add(p.docId);

                // write to temp folder as initial data set
                writePointToFile(p, out);
            }
            originClusters.add(cluster);
        }
        out.flush();
        out.close();

    }

    public void writePointToFile(DocPoint p, BufferedWriter out) throws IOException {
        for (Map.Entry<Integer, Double> itr : p.words.entrySet()) {
            out.write("" + p.docId + fieldDelimiter + itr.getKey() + fieldDelimiter + itr.getValue());
            out.newLine();
        }
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    @Test
    public void testKmeansppAccuracy() throws IOException {
        new KMeansppBagOfWords(sc, k, numIterations, new File(tmpFolder.getRoot(), dataFileName).getAbsolutePath(), outputDir).run();

        File clusterFile = new File(outputDir + KMeansppBagOfWords.CLUSTER_OUTPUT_PATH + resultFileName);

        // read clusters formed by K-Means++ from result file
        BufferedReader reader = new BufferedReader(new FileReader(clusterFile));
        List<HashSet<Integer>> clusters = new ArrayList<HashSet<Integer>>(k);
        for (int i = 0; i <= k; ++i) {
            clusters.add(new HashSet<Integer>());
        }
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.replaceAll("\\(", "").replaceAll("\\)", "").split(",");

            int clusterId = Integer.parseInt(fields[0]);
            int pointId = Integer.parseInt(fields[1]);

            clusters.get(clusterId).add(pointId);
        }
        reader.close();

        // compare the original clusters with the formed ones
        for (HashSet<Integer> cluster : clusters) {
            if (cluster.isEmpty())
                continue;
            boolean match = false;
            for (Cluster c : originClusters) {
                match = match || hashsetEquals(cluster, c.pointIds);
            }
            assertTrue("The cluster formed by K-Means++ does not match the original cluster!", match);
        }
    }

    public boolean hashsetEquals(HashSet<Integer> s1, HashSet<Integer> s2) {
        if (s1.size() != s2.size()) {
            return false;
        }
        for (int itr : s1) {
            if (!s2.contains(itr))
                return false;
        }
        return true;
    }

    private static class Cluster implements Serializable {

        private static final long serialVersionUID = -2779412714237240261L;

        public DocPoint center = new DocPoint();

        public double radius;

        public HashSet<Integer> pointIds = new HashSet<Integer>();
    }

    private boolean intersect(double c1, double r1, double c2, double r2) {
        if (c1 < c2) {
            return c1 + r1 >= c2 - r2;
        } else {
            return c2 + r2 >= c1 - r1;
        }
    }

    private boolean intersectWithExistingCenter(Cluster c, Collection<Cluster> exists) {
        if (exists == null || exists.isEmpty()) {
            return false;
        }

        for (Cluster itr : exists) {
            for (Map.Entry<Integer, Double> attr : itr.center.words.entrySet()) {
                if (!intersect(c.center.words.get(attr.getKey()), c.radius, attr.getValue(), itr.radius)) {
                    return false;
                }
            }
        }
        return true;
    }
}
