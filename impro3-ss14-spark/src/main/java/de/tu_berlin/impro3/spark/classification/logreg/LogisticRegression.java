package de.tu_berlin.impro3.spark.classification.logreg;

import java.io.FileWriter;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class LogisticRegression {

	// --------------------------------------------------------------------------------------------
	// ------------------------------ Data Structures ---------------------------------------------
	// --------------------------------------------------------------------------------------------

	/**
	 * This class represents a labeled point in space. It consists of a feature
	 * vector (represented as array of doubles) and an Integer label.
	 */
	public static class Point {

		Integer f0;
		double[] f1;

		/**
		 * Returns the vector of features
		 * 
		 * @return vector of features
		 */
		public double[] getFeatures() {
			return this.f1;
		}

		/**
		 * Returns the feature at position i
		 * 
		 * @return feature at position i
		 */
		public double getFeature(int i) {
			return this.f1[i];
		}

		/**
		 * Sets the feature vector
		 */
		public void setFeatures(double[] features) {
			this.f1 = features;
		}

		/**
		 * Returns the label
		 * 
		 * @return label
		 */
		public Integer getLabel() {
			return this.f0;
		}

		/**
		 * Sets the label
		 * 
		 * @param label
		 */
		public void setLabel(Integer label) {
			this.f0 = label;
		}

	}

	/**
	 * This is a convenience class that represents a vector of doubles
	 */
	@SuppressWarnings("serial")
	public static class Vector implements Serializable {

		double[] f0;

		public Vector() {
			// default constructor needed for instantiation during serialization
		}

		/**
		 * Constructs a new Vector of given size
		 */
		public Vector(int size) {
			double[] components = new double[size];
			for (int i = 0; i < size; i++) {
				components[i] = 0.0;
			}
			setComponents(components);
		}

		/**
		 * Returns the double vector that represents the components of this
		 * Vector
		 */
		public double[] getComponents() {
			return this.f0;
		}

		/**
		 * Returns the component at position i
		 */
		public double getComponent(int i) {
			return this.f0[i];
		}

		/**
		 * Sets the component at position i with the given value
		 */
		public void setComponent(int i, double value) {
			this.f0[i] = value;
		}

		/**
		 * Sets the whole component vector
		 */
		public void setComponents(double[] components) {
			this.f0 = components;
		}

	}

	/**
	 * A Vector that represents the Theta that the optimization is working on
	 */
	@SuppressWarnings("serial")
	public static class Theta extends Vector {

		public Theta() {
			// default constructor needed for instantiation during serialization
		}

		public Theta(int size) {
			super(size);
		}
	}

	/**
	 * A Vector that represents the gradient of a given point
	 */
	@SuppressWarnings("serial")
	public static class Gradient extends Vector {

		public Gradient() {
			// default constructor needed for instantiation during serialization
		}

		public Gradient(int size) {
			super(size);
		}
	}

	// --------------------------------------------------------------------------------------------
	// ------------------------------ Main Program ------------------------------------------------
	// --------------------------------------------------------------------------------------------

	/**
	 * This is the main program for logistic regression. This methods reads the input arguments
	 * and starts the actual calculation by calling ({@link #runProgram(JavaSparkContext, int, String, float, int, String, int)}
	 * @param args numberOfFeatures, inputPathToPoints, alpha, maxIterations, outputPath
	 * @throws Exception All exceptions are thrown
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.out.println("You did not provide enough parameters. Please run the program with the following parameters:");
			System.out
					.println("[number of features] [path to input points] [learning rate alpha] [number of iterations] [output path] [optional: position of label in CSV]");
		}

		final int numberOfFeatures = Integer.valueOf(args[0]);
		String pointsWithLabelsPath = args[1];
		float alpha = Float.valueOf(args[2]);
		int maxIterations = Integer.valueOf(args[3]);
		String outputPathTheta = args[4];

		int labelPosition = 1;
		if (args.length > 5) {
			labelPosition = Integer.valueOf(args[5]);
		}

		SparkConf conf = new SparkConf().setAppName("LogReg");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		runProgram(sc, numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition);

	}

	/**
	 * runs the logistic regression program with the given parameters
	 * @param sc The Spark context provided as {@link JavaSparkContext}
	 * @param numberOfFeatures
	 * @param pointsWithLabelsPath
	 * @param alpha
	 * @param maxIterations
	 * @param outputPathTheta
	 * @param labelPosition
	 * @throws Exception
	 */
	public static void runProgram(final JavaSparkContext sc, final int numberOfFeatures, String pointsWithLabelsPath, float alpha, int maxIterations, String outputPathTheta,
			int labelPosition) throws Exception {
		JavaRDD<Point> pointsWithLabels = parsePointsWithLabels(sc, pointsWithLabelsPath, numberOfFeatures, labelPosition).cache();

		//Calculate the total number of points present in the training set
		@SuppressWarnings("serial")
		final Integer numberOfPoints = pointsWithLabels.map(new Function<Point, Integer>() {

			@Override
			public Integer call(Point p) throws Exception {
				return 1;
			}

		}).reduce(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer i1, Integer i2) throws Exception {
				return i1 + i2;
			}

		});

		final Theta theta = new Theta(numberOfFeatures);

		//Main iteration: Do maxInteration iteration before the result is returned
		for (int i = 0; i < maxIterations; i++) {

			//Calculate the sum of the gradients
			@SuppressWarnings("serial")
			Gradient sumGradient = pointsWithLabels.map(new Function<Point, Gradient>() {

				@Override
				public Gradient call(Point pointWithLabel) throws Exception {
					Gradient gradient = new Gradient(numberOfFeatures);
					
					double[] pointVector=pointWithLabel.getFeatures();
					double[] thetaVector=theta.getComponents();
					
					for (int j = 0; j < gradient.getComponents().length; j++) {
						gradient.setComponent(j,
								((sigmoid(dotProduct(thetaVector, pointVector)) - pointWithLabel.getLabel())
										* pointWithLabel.getFeature(j)) / numberOfPoints);
					}

					return gradient;
				}
			}).reduce(new Function2<LogisticRegression.Gradient, LogisticRegression.Gradient, LogisticRegression.Gradient>() {

				@Override
				public Gradient call(Gradient grad0, Gradient grad1) throws Exception {
					// grad0 += grad1
					for (int i = 0; i < grad0.getComponents().length; i++) {
						grad0.setComponent(i, grad0.getComponent(i) + grad1.getComponent(i));
					}

					return grad0;
				}

			});

			//Update theta using the gradient sum, the total number of points in the training set, and alpha
			theta.setComponents(subtract(theta.getComponents(), mapMultiplyToSelf(sumGradient.getComponents(), alpha)));

		}

		//Print the result theta after all iterations are done
		System.out.println("The calculated theta is:");
		StringBuilder sb = new StringBuilder();
		for (double d : theta.getComponents()) {
			System.out.println(d);
			sb.append(Double.toString(d));
			sb.append(',');
		}
		
		//Write the calculated theta to the given output path
		sb.deleteCharAt(sb.length() -1 );
		FileWriter fw = new FileWriter(outputPathTheta + "/0");
		fw.append(sb.toString());
		fw.close();

	}

	// --------------------------------------------------------------------------------------------
	// ------------------------------ Utility methods ---------------------------------------------
	// --------------------------------------------------------------------------------------------

	/**
	 * This method is used to parse an input text file into an DataSet of Points.
	 * Label position specifies the position of the label value in the CSV file
	 * @param sc Spark context provided as {@link JavaSparkContext}
	 * @param pointsWithLabelsPath
	 * @param numberOfFeatures
	 * @param labelPosition
	 * @return DataSet of Points
	 */
	@SuppressWarnings("serial")
	public static JavaRDD<Point> parsePointsWithLabels(JavaSparkContext sc, String pointsWithLabelsPath, final int numberOfFeatures,
			final int labelPosition) {
		return sc.textFile(pointsWithLabelsPath).map(new Function<String, Point>() {

			@Override
			public Point call(String line) throws Exception {
				Point p = new Point();

				String[] split = line.split(",");
				double[] features = new double[numberOfFeatures];

				int a = 0;
				for (int i = 0; i < split.length; i++) {

					if (i == labelPosition - 1) {
						p.setLabel(new Integer(split[i].trim().substring(0, 1)));
					} else {
						if (a < numberOfFeatures && split[i].trim() != "") {
							features[a++] = Double.parseDouble(split[i].trim());
						}
					}
				}

				p.setFeatures(features);
				return p;
			}

		});
	}

	/**
	 * Calculates the sigmoid
	 */
	public static double sigmoid(double x) {
		return 1.0 / (1.0 + Math.pow(Math.E, -x));
	}
	
	/**
	 * Copied from apache commons math3
	 *
	 * @param v
	 * @return
	 */
	public static double dotProduct(double a[], double b[]) {
		double dot = 0;
		for (int i = 0; i < b.length; i++) {
			dot += b[i] * a[i];
		}
		return dot;
	}

	/**
	 * Copied from apache commons math3
	 *
	 * @param v
	 * @return
	 */
	public static double[] subtract(double a[], double b[]) {
		final int dim = b.length;
		double[] resultData = new double[dim];
		for (int i = 0; i < dim; i++) {
			resultData[i] = a[i] - b[i];
		}
		return resultData;
	}

	/**
	 * Copied from apache commons math3
	 *
	 * @param a
	 * @param d
	 * @return
	 */
	public static double[] mapMultiplyToSelf(double a[], double d) {
		for (int i = 0; i < a.length; i++) {
			a[i] *= d;
		}
		return a;
	}

}

