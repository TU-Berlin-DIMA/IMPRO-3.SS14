package de.tu_berlin.impro3.stratosphere.clustering.kmeansplusplus;

import de.tu_berlin.impro3.stratosphere.clustering.kmeanspp.KMeansppDouble;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.Assert;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class KMeansppDoubleTest extends JavaProgramTestBase {

	String data = "1|-8.35|-45.34|\n" +
		"2|-31.40|35.75|\n" +
		"3|47.68|-46.61|\n" +
		"4|29.03|45.19|\n" +
		"5|-6.59|-44.68|\n" +
		"6|-35.73|39.93|\n" +
		"7|47.54|-52.04|\n" +
		"8|23.78|46.82|\n" +
		"9|-8.13|-43.18|\n" +
		"10|-36.57|34.08|\n" +
		"11|45.61|-50.14|\n" +
		"12|25.07|39.10|\n" +
		"13|-4.02|-40.84|\n" +
		"14|-37.40|35.56|\n" +
		"15|35.61|-50.64|\n" +
		"16|28.05|35.69|\n" +
		"17|-4.64|-48.68|\n" +
		"18|-31.36|32.87|\n" +
		"19|48.74|-45.32|\n" +
		"20|25.07|43.24|\n" +
		"21|-1.05|-42.79|\n" +
		"22|-31.48|36.15|\n" +
		"23|43.32|-49.80|\n" +
		"24|21.37|48.48|\n" +
		"25|-0.24|-48.35|\n" +
		"26|-32.84|32.70|\n" +
		"27|44.38|-47.26|\n" +
		"28|26.79|45.48|\n" +
		"29|-2.80|-39.30|\n" +
		"30|-37.51|29.63|\n" +
		"31|42.73|-48.85|\n" +
		"32|28.78|44.74|\n" +
		"33|-11.23|-45.71|\n" +
		"34|-40.36|39.10|\n" +
		"35|44.81|-46.86|\n" +
		"36|27.16|44.80|\n" +
		"37|-1.50|-43.67|\n" +
		"38|-37.05|37.48|\n" +
		"39|40.95|-48.61|\n" +
		"40|23.70|48.03|\n" +
		"41|-2.18|-42.42|\n" +
		"42|-37.79|40.16|\n" +
		"43|37.77|-47.42|\n" +
		"44|22.73|47.34|\n" +
		"45|-5.46|-48.18|\n" +
		"46|-37.17|36.92|\n" +
		"47|44.41|-47.72|\n" +
		"48|20.61|39.36|\n" +
		"49|-3.34|-41.42|\n" +
		"50|-37.28|34.82|\n" +
		"51|40.52|-47.63|\n" +
		"52|30.87|45.12|\n" +
		"53|-1.61|-43.29|\n" +
		"54|-39.41|29.86|\n" +
		"55|49.65|-46.92|\n" +
		"56|26.17|44.77|\n" +
		"57|-1.84|-40.63|\n" +
		"58|-43.00|34.16|\n" +
		"59|46.55|-49.29|\n" +
		"60|21.42|43.29|\n" +
		"61|-2.97|-41.63|\n" +
		"62|-39.98|35.85|\n" +
		"63|35.83|-48.65|\n" +
		"64|22.07|36.59|\n" +
		"65|-2.75|-44.09|\n" +
		"66|-33.85|40.27|\n" +
		"67|38.50|-47.34|\n" +
		"68|27.88|43.19|\n" +
		"69|-6.47|-45.49|\n" +
		"70|-34.35|31.70|\n" +
		"71|44.11|-49.59|\n" +
		"72|27.65|41.23|\n" +
		"73|-6.63|-38.39|\n" +
		"74|-42.32|33.99|\n" +
		"75|46.08|-53.83|\n" +
		"76|26.79|41.43|\n" +
		"77|-3.55|-38.43|\n" +
		"78|-36.04|34.85|\n" +
		"79|44.71|-47.94|\n" +
		"80|22.70|42.88|\n" +
		"81|-7.25|-44.57|\n" +
		"82|-39.32|38.41|\n" +
		"83|38.12|-51.58|\n" +
		"84|24.32|46.29|\n" +
		"85|-1.11|-45.89|\n" +
		"86|-42.10|33.34|\n" +
		"87|42.90|-49.17|\n" +
		"88|27.08|41.45|\n" +
		"89|-3.31|-44.42|\n" +
		"90|-30.65|30.31|\n" +
		"91|41.03|-49.20|\n" +
		"92|23.51|42.16|\n" +
		"93|-7.05|-45.95|\n" +
		"94|-36.03|42.24|\n" +
		"95|36.48|-48.07|\n" +
		"96|21.99|41.74|\n" +
		"97|-5.52|-46.07|\n" +
		"98|-35.74|39.24|\n" +
		"99|43.69|-47.31|\n" +
		"100|22.00|41.23|\n";
	String [] expectedResult = {"1|-4.3836|-43.7364",
		"2|42.8688|-48.7116",
		"3|-36.6692|35.5748",
		"4|25.0636|43.1856"};

	String inputPath;
	String outputPath;
	int k = 4;
	int numIterations = 100;

	@Override
	public void preSubmit() throws Exception {
		inputPath = createTempFile("KMeansppDoubleInput", data);
		outputPath = getTempDirPath("KMeansppDoubleResult");
	}

	@Override
	public void testProgram() throws Exception {
		new KMeansppDouble(k,
			numIterations,
			inputPath,
			outputPath).run();
		validate();
	}

	public void validate() throws Exception {
		List<String> result = new ArrayList<String>();
		readAllResultLines(result, outputPath + "/centers", false);
		for (String centerWrapper: expectedResult) {
			String[] centerExpected = centerWrapper.split("\\|");
			double x = Double.parseDouble(centerExpected[centerExpected.length - 2]);
			double y = Double.parseDouble(centerExpected[centerExpected.length - 1]);
			boolean flag = false;
			for (String centerResultWrapper: result) {
				String[] centerResult = centerResultWrapper.split("\\|");
				double xx = Double.parseDouble(centerResult[centerResult.length - 2]);
				double yy = Double.parseDouble(centerResult[centerResult.length - 1]);
				if (Math.sqrt((x - xx)*(x - xx) + (y - yy)*(y - yy)) < 5) {
					flag = true;
				}
			}
			Assert.assertTrue("no match for center " + centerWrapper, flag);
		}
	}

	@Override
	public void postSubmit() throws Exception {
		File outputFile = new File(outputPath);
		if (outputFile.exists()) {
			deleteDirectory(outputFile);
		}
		File inputFile = new File(inputPath);
		if (inputFile.exists()) {
			inputFile.delete();
		}
	}

	public boolean deleteDirectory(File directory) {
		if(directory.exists()){
			File[] files = directory.listFiles();
			if(null!=files){
				for(int i=0; i<files.length; i++) {
					if(files[i].isDirectory()) {
						deleteDirectory(files[i]);
					}
					else {
						files[i].delete();
					}
				}
			}
		}
		return(directory.delete());
	}
}
