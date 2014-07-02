/*
 * (C) Copyright TU-Berlin DIMA and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     - oresti
 *     - seekermrm
 */
package de.tu_berlin.impro3.stratosphere.clustering.canopy;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class CanopyClusteringTest {

    // docID wordId
    private static final String tinyInput =
            "1 100\n1 101\n1 102\n" +
            "2 100\n2 101\n2 105\n" +
            "3 106\n3 107\n" +
            "4 107\n" +
            "5 106\n";

    private static final String tinyOutput =
            "1,2\n3,4,5\n";

    /**
     * This one may be used to verify the output.
     */
    @Test
    public void testTinyData() throws Exception {
        String in = "/tmp/testData.txt";
        String out = "/tmp/output-canopy-tinyTest.csv";
        FileUtils.writeStringToFile(new File(in), tinyInput);
        String[] args = {"1", in, out, "0.15", "0.45"};
        Canopy.main(args);

        final String result = FileUtils.readFileToString(new File(out));
        Assert.assertTrue(result.equals(tinyOutput));
    }

    /**
     * There should be 317 canopies for these thresholds and kos.txt.
     */
    @Test
    public void testKosData() throws Exception {
        String kosData = this.getClass().getResource("/datasets/clustering/bag-of-words/docword.kos.txt").toURI().toString();
        String[] args = {"1", kosData, "/tmp/output-canopy-kos.csv", "0.06", "0.1"};
        Canopy.main(args);

        final String result = FileUtils.readFileToString(new File("/tmp/output-canopy-kos.csv"));
        String[] lines = result.split("\r\n|\r|\n");
        Assert.assertTrue(lines.length == 317);
    }
}
