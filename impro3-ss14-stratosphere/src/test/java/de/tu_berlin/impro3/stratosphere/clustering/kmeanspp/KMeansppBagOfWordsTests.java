package de.tu_berlin.impro3.stratosphere.clustering.kmeanspp;

import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import eu.stratosphere.core.fs.Path;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.After;


public class KMeansppBagOfWordsTests extends JavaProgramTestBase {

    public static final long seed = 14839204302832L;

    public static final int numPointsPerCluster = 300;

    public static final int numDimensions = 200;

    public static final int k = 5;

    public static final int numIterations = 10;

    public static final int maxClusterRadius = 200;

    public static final int maxFeatureValue = 10000;


    public static final String fieldDelimiter = " ";

    private String inputPath;

    private String outputPath;

    Random random = new Random(seed);

    List<Cluster> originClusters = new ArrayList<Cluster>(k);

    @Override
    public void preSubmit() throws IOException {
        inputPath = createAndRegisterTempFile("KMeansppInput").getPath();
        BufferedWriter out = new BufferedWriter(new FileWriter(new File(inputPath)));

        outputPath = getTempDirPath("KMeansResult");

        // generate initial data set
        for (int c = 0; c < k; ++c) {
            // pick a new center that does not intersect with existing centers
            Cluster cluster = new Cluster();
            cluster.radius = (double) random.nextInt(maxClusterRadius) + 1;
            do {
                for (int w = 0; w < numDimensions; ++w) {
                    cluster.center.wordFreq.put(Integer.toString(w), (double) random.nextInt(maxFeatureValue));
                }
            } while (intersectWithExistingCenter(cluster, originClusters));

            // add points to the new center
            for (int i = 0; i < numPointsPerCluster; ++i) {
                int docId = i + numPointsPerCluster * c;
                KMeansppBagOfWords.Document p = new KMeansppBagOfWords.Document(docId);
                for (int w = 0; w < numDimensions; ++w) {
                    // generate a point whose word counts are within the range cluster.center +/-
                    // radius
                    int freq = (int) (random.nextInt((int) (cluster.radius * 2)) + cluster.center.wordFreq.get(Integer.toString(w)) - cluster.radius);
                    p.wordFreq.put(Integer.toString(w), freq > 0 ? (double) freq : 0);
                }
                cluster.pointIds.add(p.id);

                // write to temp folder as initial data set
                writePointToFile(p, out);
            }
            originClusters.add(cluster);
        }
        out.flush();
        out.close();

    }

    public void writePointToFile(KMeansppBagOfWords.Document p, BufferedWriter out) throws IOException {
        for (Map.Entry<String, Double> itr : p.wordFreq.entrySet()) {
            out.write("" + p.id + fieldDelimiter + itr.getKey() + fieldDelimiter + itr.getValue());
            out.newLine();
        }
    }

    @After
    public void tearDown() {}

    @Override
    public void testProgram() throws Exception {
        new KMeansppBagOfWords(k, numIterations, inputPath, outputPath).run();

        ArrayList<String> list = new ArrayList<>();
        readAllResultLines(list, new Path(outputPath, "clusters").toString(), false);

        // read clusters formed by K-Means++ from result file
        List<HashSet<Integer>> clusters = new ArrayList<>(k);
        for (int i = 0; i <= k; ++i) {
            clusters.add(new HashSet<Integer>());
        }
        for (String line : list) {
            String[] fields = line.split("\\|");

            int clusterId = Integer.parseInt(fields[0]);
            int pointId = Integer.parseInt(fields[1]);

            clusters.get(clusterId).add(pointId);
        }

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

        public KMeansppBagOfWords.Document center = new KMeansppBagOfWords.Document();

        public double radius;

        public HashSet<Integer> pointIds = new HashSet<>();
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
            for (Map.Entry<String, Double> attr : itr.center.wordFreq.entrySet()) {
                if (!intersect(c.center.wordFreq.get(attr.getKey()), c.radius, attr.getValue(), itr.radius)) {
                    return false;
                }
            }
        }
        return true;
    }
}
