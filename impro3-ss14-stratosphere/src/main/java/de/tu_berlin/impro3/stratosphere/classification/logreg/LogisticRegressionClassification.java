package de.tu_berlin.impro3.stratosphere.classification.logreg;

import java.util.Iterator;

import de.tu_berlin.impro3.stratosphere.classification.logreg.LogisticRegression.Point;
import de.tu_berlin.impro3.stratosphere.classification.logreg.LogisticRegression.Theta;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;

/**
 * 
 * This class is used for classification of data with an previously obtained theta
 *
 */
@SuppressWarnings("serial")
public class LogisticRegressionClassification {
	
	// --------------------------------------------------------------------------------------------
	// ------------------------------ Data Structures ---------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Represents a point with a classification value
	 */
	public static class ClassifiedPoint extends Tuple2<Double, Point> {
		
		public void setPoint(Point p) {
			this.f1 = p;
		}
		
		public Point getPoint() {
			return this.f1;
		}
		
		public void setClassification(double classification) {
			this.f0 = new Double(classification);
		}
		
		public double getClassification() {
			return this.f0.doubleValue();
		}
	}


	// --------------------------------------------------------------------------------------------
	// ------------------------------ Main Program ------------------------------------------------
	// --------------------------------------------------------------------------------------------

	// args: input points, input theta, ouptut classified points
	public static void main(String[] args) throws Exception {
		
		if(args.length < 4) {
			System.out.println("You did not provide enough parameters. Please run the program with the following parameters:");
			System.out.println("[number of features] [path to input points] [path to input theta] [output path] [optional: position of label in CSV]");
		}

		final int numberOfFeatures = Integer.valueOf(args[0]);
		String pointsWithLabelsPath = args[1];
		String thetaPath = args[2];
		String outputPath = args[3];
		
		int labelPosition = 1;
		if(args.length > 4) {
			labelPosition = Integer.valueOf(args[4]);
		}
		
		runProgram(numberOfFeatures, pointsWithLabelsPath, thetaPath, outputPath, labelPosition);
	}
	
	/**
	 * runs the logist regression classification program with the given parameters
	 * 
	 * @param pointsWithLabelsPath
	 * @param thetaPath
	 * @param outputPath
	 * @param labelPosition
	 * @throws Exception 
	 */
	public static void runProgram(int numberOfFeatures, String pointsWithLabelsPath, String thetaPath, String outputPath, int labelPosition) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Point> pointsWithLabels = LogisticRegression.parsePointsWithLabels(env, pointsWithLabelsPath, numberOfFeatures, labelPosition);
		
		DataSet<Theta> theta = env.readTextFile(thetaPath).map(new MapFunction<String, Theta>() {

			/**
			 * Parses the input file into a Theta object
			 */
			@Override
			public Theta map(String value) throws Exception {

				Theta t = new Theta();
				
				String[] split = value.split(",");
				double[] features = new double[split.length];
				
				for(int i = 0; i < split.length; i++) {
					features[i] = Double.parseDouble(split[i].replaceAll("[^0-9.,-]+",""));
				}
				
				t.setComponents(features);
				return t;
			}
			
		});
		
		DataSet<ClassifiedPoint> classification = pointsWithLabels.map(new MapFunction<Point, ClassifiedPoint>() {

			Theta theta;
			
			ClassifiedPoint point; // used as return value for performance reasons
			
			public void open(Configuration parameters) throws Exception {
				// load broadcast variable for theta
				theta = (Theta) getRuntimeContext().getBroadcastVariable("theta").iterator().next();
				
				point = new ClassifiedPoint();
			}
			
			/**
			 * Calculates the classification with use of given theta
			 */
			@Override
			public ClassifiedPoint map(Point pointWithLabel) throws Exception {

				double classification = LogisticRegression.sigmoid(LogisticRegression.dotProduct(theta.getComponents(), pointWithLabel.getFeatures()));
				
				point.setClassification(classification);
				point.setPoint(pointWithLabel);
				
				return point;
			}
		})
		.withBroadcastSet(theta, "theta");
		
		classification.writeAsCsv(outputPath);
		
		DataSet<Integer> numberOfCorrectClassifications = classification.reduceGroup(new GroupReduceFunction<ClassifiedPoint, Integer>() {

					/**
					 * Builds the sum of all correctly classified points
					 */
					@Override
					public void reduce(Iterator<ClassifiedPoint> values,
							Collector<Integer> out) throws Exception {
						
						int cnt = 0;
						while(values.hasNext()) {
							
							ClassifiedPoint p = values.next();
							
							if(p.getPoint().getLabel() == 0) {
								if(p.getClassification() < 0.5) {
									cnt++;
								}
							}
							else {
								if(p.getClassification() >= 0.5) {
									cnt++;
								}
							}
						}
						
						out.collect(new Integer(cnt));
					}
				});

		numberOfCorrectClassifications.print();

		env.execute("Logistic Regression");
		
		System.out.println("= Number of correct classified points");

	}
}
