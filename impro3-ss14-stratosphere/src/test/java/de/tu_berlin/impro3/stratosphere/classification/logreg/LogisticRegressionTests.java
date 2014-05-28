package de.tu_berlin.impro3.stratosphere.classification.logreg;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import junit.framework.Assert;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.test.util.JavaProgramTestBase;

@RunWith(Parameterized.class)
public class LogisticRegressionTests extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 5;
	
	private int curProgId = config.getInteger("ProgramId", -1);
	private static String resultPath;
	private static String resultPath2;
	private static String resourcePath;
	
	public static String theta;
	
	public LogisticRegressionTests(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
		resultPath2 = getTempDirPath("result2");
		resourcePath =  getClass().getClassLoader().getResource(".").getPath()+"/../classes/";
	}
	
	@Override
	protected void testProgram() throws Exception {
		runProgram(curProgId, resultPath, resultPath2);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}

	public void runProgram(int progId, String resultPath , String resultPath2) throws Exception {
		
		switch(progId) {
			case 1: {
				
				/*
				 * The first three tests test whether or not three predefined datasets produce the correct theta value.
				 * The correctness of the theta value was validated through a scala and a Matlab implementation of the
				 * logist regression algorithm.
				 */

				int numberOfFeatures = 1;
				String pointsWithLabelsPath = resourcePath+"cub_gen_output_10";
				float alpha = 1;
				int maxIterations = 5000;
				String outputPathTheta = resultPath;
				int labelPosition = 2;
				
				LogisticRegression.runProgram(numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition);
				
				ArrayList<String> list = new ArrayList<String>();
				readAllResultLines(list, resultPath, false);
				
				double result = Double.parseDouble(list.get(0).replaceAll("[^0-9.,-]+",""));
				
				Assert.assertTrue(result > 6.0 && result < 6.2);
				break;
			}
			case 2: {

				int numberOfFeatures = 1;
				String pointsWithLabelsPath = resourcePath+"cub_gen_output_100";
				float alpha = 1;
				int maxIterations = 5000;
				String outputPathTheta = resultPath;
				int labelPosition = 2;
				
				LogisticRegression.runProgram(numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition);
				
				ArrayList<String> list = new ArrayList<String>();
				readAllResultLines(list, resultPath, false);
				
				double result = Double.parseDouble(list.get(0).replaceAll("[^0-9.,-]+",""));
				
				Assert.assertTrue(result > 1.2 && result < 1.4);
				break;
			}
			case 3: {

				int numberOfFeatures = 1;
				String pointsWithLabelsPath = resourcePath+"cub_gen_output_1000";
				float alpha = 1;
				int maxIterations = 5000;
				String outputPathTheta = resultPath;
				int labelPosition = 2;
				
				LogisticRegression.runProgram(numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition);
				
				ArrayList<String> list = new ArrayList<String>();
				readAllResultLines(list, resultPath, false);
				
				double result = Double.parseDouble(list.get(0).replaceAll("[^0-9.,-]+",""));
				
				Assert.assertTrue(result > 1.6 && result < 1.8);
				break;
			}
			case 4: {
				
				/*
				 * Preprocessing step for 5
				 */
				int numberOfFeatures = 2;
				String pointsWithLabelsPath = resourcePath+"generated_sample";
				float alpha = 0.01f;
				int maxIterations = 5000;
				String outputPathTheta = resultPath;
				int labelPosition = 1;
				
				LogisticRegression.runProgram(numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition);
				
				ArrayList<String> list = new ArrayList<String>();
				readAllResultLines(list, resultPath, false);
				
				for(String s : list) {
					theta = s;
				}
				
				break;
			}
			case 5: {
				
				/*
				 * Tests if a artificially generated dataset is 100% correct classified
				 */

				resultPath = createTempFile("theta.txt", theta);
				
				int numberOfFeatures = 2;
				String pointsWithLabelsPath = resourcePath+"generated_sample";
				int labelPosition = 1;

				LogisticRegressionClassification.runProgram(numberOfFeatures, pointsWithLabelsPath, resultPath, resultPath2, labelPosition);
				
				ArrayList<String> list = new ArrayList<String>();
				readAllResultLines(list, resultPath2, false);
				
				for(String s : list) {
					
					String[] split = s.split(",");
					
					if(Double.parseDouble(split[0]) < 0.5 ) {
						Assert.assertTrue(split[1].replaceAll("[^0-9.,-]+","").equals("0"));
					}
					else if(Double.parseDouble(split[0]) >= 0.5 ) {
						Assert.assertTrue(split[1].replaceAll("[^0-9.,-]+","").equals("1"));
					}
					
				}
				
				break;
			}
			default: 
				throw new IllegalArgumentException("Invalid program id "+progId);
		}
	}
}
