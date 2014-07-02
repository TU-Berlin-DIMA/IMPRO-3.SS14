package de.tu_berlin.impro3.stratosphere.clustering.canopy;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class CanopyClusteringTest extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 2;

    private int curProgId = config.getInteger("ProgramId", -1);

    // docID wordId
    private static final String tinyInput =
                    "1 100\n1 101\n1 102\n" +
                    "2 100\n2 101\n2 105\n" +
                    "3 106\n3 107\n" +
                    "4 107\n" +
                    "5 106\n";

    private static final String tinyOutput = "1,2\n3,4,5\n";

    public CanopyClusteringTest(Configuration config) {
        super(config);
    }

    @Override
    protected void testProgram() throws Exception {
        runProgram(curProgId);
    }

    @Parameters
    public static Collection<Object[]> getConfigurations() throws IOException {

        LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

        for (int i = 1; i <= NUM_PROGRAMS; i++) {
            Configuration config = new Configuration();
            config.setInteger("ProgramId", i);
            tConfigs.add(config);
        }

        return toParameterList(tConfigs);
    }

    public void runProgram(int progId) throws Exception {

        switch (progId) {
            case 1: {
                String in = "/tmp/testData.txt";
                String out = "/tmp/output-canopy-tinyTest.csv";
                FileUtils.writeStringToFile(new File(in), tinyInput);
                String[] args = {"1", in, out, "0.15", "0.45"};
                Canopy.main(args);

                final String result = FileUtils.readFileToString(new File(out));
                Assert.assertTrue(result.equals(tinyOutput));

                break;
            }
            case 2: {
                String kosData = this.getClass().getResource("/datasets/clustering/bag-of-words/docword.kos.txt").toURI().toString();
                String[] args = {"1", kosData, "/tmp/output-canopy-kos.csv", "0.06", "0.1"};
                Canopy.main(args);

                final String result = FileUtils.readFileToString(new File("/tmp/output-canopy-kos.csv"));
                String[] lines = result.split("\r\n|\r|\n");
                Assert.assertTrue(lines.length == 317);

                break;
            }
            default:
                throw new IllegalArgumentException("Invalid program id " + progId);
        }
    }
}
