package de.tu_berlin.impro3.spark.clustering.hac;

import java.io.File;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import scala.Enumeration.Value;
import scala.Tuple2;

import com.google.common.io.Files;

/**
 * @author Sascha
 */
public class HACTest {

    //@formatter:off
    private final static int[][] RESULT_MINI_SINGLE_LINKAGE = new int[][] {
            new int[] {1, 2, 3, 4, 5, 6}, // 0 iteration: doc id = cluster id
            new int[] {1, 2, 3, 5, 5, 6}, // 1 iteration: 4 => 5
            new int[] {5, 2, 3, 5, 5, 6}, // 2 iteration: 1 => 5
            new int[] {6, 2, 3, 6, 6, 6}, // 3 iteration: 5 => 6
            new int[] {6, 2, 6, 6, 6, 6}, // 4 iteration: 3 => 6
            new int[] {6, 6, 6, 6, 6, 6} // 5 iteration: 2 => 6
    };

    private final static int[][] RESULT_MINI_COMPLETE_LINKAGE = new int[][] {
            new int[] {1, 2, 3, 4, 5, 6}, // 0 iteration: doc id = cluster id
            new int[] {1, 2, 3, 6, 5, 6}, // 1 iteration: 4 => 6
            new int[] {1, 6, 3, 6, 5, 6}, // 1 iteration: 2 => 6
            new int[] {1, 6, 6, 6, 5, 6}, // 1 iteration: 3 => 6
            new int[] {6, 6, 6, 6, 5, 6}, // 1 iteration: 1 => 6
            new int[] {6, 6, 6, 6, 6, 6} // 1 iteration: 5 => 6
    };
    //@formatter:on

    private static JavaSparkContext sc;

    private File outputDir;

    @BeforeClass
    public static void setupSpark() {
        sc = new JavaSparkContext("local", "HACTest");
    }

    @AfterClass
    public static void tearDownSpark() {
        sc.stop();
    }

    @Before
    public void setupOutputDir() {
        this.outputDir = Files.createTempDir();
        this.outputDir.deleteOnExit();
    }

    public String getAbsoultOutputPath(int iterations, Value linkage) {
        return this.outputDir.getAbsolutePath() + "/output-" + iterations + "-iterations-" + linkage;
    }

    public String getMiniDataSetPath() {
        return getClass().getResource("../../../../../../datasets/clustering/bag-of-words/docword.kos.txt.mini").getFile();
    }

    public void assertEquals(Value linkage, int iterations, int expected[], List<Tuple2<Object, Object>> actual) {
        for (Tuple2<Object, Object> r : actual) {
            int doc = (int) r._2();
            Assert.assertEquals("Document " + doc + " with " + linkage + " and " + iterations + " iterations cluster id",
                                expected[doc - 1],
                                (int) r._1());
        }
    }

    @Test
    public void testSingleLinkage() throws Exception {
        for (int i = 0; i < 6; i++) {
            HAC hac = new HAC(getMiniDataSetPath(), getAbsoultOutputPath(i, LinkageMode.SingleLinkage()), i, LinkageMode.SingleLinkage());
            List<Tuple2<Object, Object>> sparkResult = hac.runProgramm(sc.sc());
            assertEquals(LinkageMode.SingleLinkage(), i, RESULT_MINI_SINGLE_LINKAGE[i], sparkResult);
        }
    }

    @Test
    public void testCompleteLinkage() throws Exception {
        for (int i = 0; i < 6; i++) {
            HAC hac = new HAC(getMiniDataSetPath(), getAbsoultOutputPath(i, LinkageMode.CompleteLinkage()), i, LinkageMode.CompleteLinkage());
            List<Tuple2<Object, Object>> sparkResult = hac.runProgramm(sc.sc());
            assertEquals(LinkageMode.CompleteLinkage(), i, RESULT_MINI_COMPLETE_LINKAGE[i], sparkResult);
        }
    }
}
