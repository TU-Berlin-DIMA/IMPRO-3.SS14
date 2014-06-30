package de.tu_berlin.impro3.stratosphere.clustering.hac;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.JavaProgramTestBase;
import junit.framework.Assert;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Scanner;

@RunWith(Parameterized.class)
public class HAC_FlinkJavaTest extends JavaProgramTestBase {

    private static int NUM_PROGRAMS = 1;

    private int curProgId = config.getInteger("ProgramId", -1);

    public static String theta;

    public HAC_FlinkJavaTest(Configuration config) {
        super(config);
    }

    @Override
    protected void testProgram() throws Exception {
        runProgram(curProgId);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

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
                TemporaryFolder testFolder = new TemporaryFolder();
                File inputFile = new File("./src/main/resources/datasets/clustering/bag-of-words/docword.kos.txt.mini");
                testFolder.create();
                String[] args = {inputFile.getCanonicalPath(), "SINGLE", testFolder.getRoot().getPath(), "false"};
                HAC_Flink_Java.main(args);

                Scanner scanner = new Scanner(new File(testFolder.getRoot().getPath()));
                String line = scanner.nextLine();
                if (!line.equals("[4; 5][1; 5][5; 6][3; 6][2; 6]")) {
                    Assert.fail("Incorrect merging by HAC");
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Invalid program id " + progId);
        }
    }
}
