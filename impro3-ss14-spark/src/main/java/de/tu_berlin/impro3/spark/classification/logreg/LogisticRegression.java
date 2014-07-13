package de.tu_berlin.impro3.spark.classification.logreg;

import java.io.FileWriter;

import de.tu_berlin.impro3.spark.Algorithm;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class LogisticRegression extends Algorithm {

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Algorithn Command -------------------------------------------
    // --------------------------------------------------------------------------------------------


    public static class Command extends Algorithm.Command<LogisticRegression> {

        /*
         * 
         * final int numberOfFeatures, String pointsWithLabelsPath, float alpha, int maxIterations,
         * String outputPathTheta, int labelPosition
         */

        public final static String KEY_NUM_FEATURES = "algorithm.logreg.features.count";

        public final static String KEY_ALPHA = "algorithm.logreg.iterations";

        public final static String KEY_ITERATIONS = "algorithm.logreg.iterations";

        public final static String KEY_LABEL_POSITION = "algorithm.logreg.label.position";

        public Command() {
            super("logreg", "Logistic Regression", LogisticRegression.class);
        }

        @Override
        public void setup(Subparser parser) {
            super.setup(parser);

            //@formatter:off
            parser.addArgument("-f", "--features")
                    .type(Integer.class)
                    .required(true)
                    .dest(KEY_NUM_FEATURES)
                    .metavar("N")
                    .help("Number of features");
            parser.addArgument("-a", "--alpha")
                    .type(Float.class)
                    .required(true)
                    .dest(KEY_ALPHA)
                    .metavar("ALPHA")
                    .help("Learning rate alpha");
            parser.addArgument("-i", "--iterations")
                    .type(Integer.class)
                    .required(true)
                    .dest(KEY_ITERATIONS)
                    .metavar("N")
                    .help("Number of iterations");
            parser.addArgument("-l", "--label-position")
                    .type(Integer.class)
                    .setDefault(1)
                    .dest(KEY_LABEL_POSITION)
                    .metavar("X")
                    .help("Position of label in CSV");
            //@formatter:on
        }
    }

    // --------------------------------------------------------------------------------------------
    // --------------------------------- Algorithm ------------------------------------------------
    // --------------------------------------------------------------------------------------------

    @Override
    public void run(Namespace ns) throws Exception {
        final int numberOfFeatures = ns.getInt(Command.KEY_ITERATIONS);
        final float alpha = ns.getFloat(Command.KEY_ALPHA);
        final int maxIterations = ns.getInt(Command.KEY_ITERATIONS);
        final int labelPosition = ns.getInt(Command.KEY_LABEL_POSITION);
        final String pointsWithLabelsPath = ns.getString(Command.KEY_INPUT);
        final String outputPathTheta = ns.getString(Command.KEY_OUTPUT);

        SparkConf conf = new SparkConf().setAppName("LogReg");
        JavaSparkContext sc = new JavaSparkContext(conf);

        runProgram(sc, numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition);
    }

    /**
     * runs the logistic regression program with the given parameters
     * 
     * @param sc The Spark context provided as {@link JavaSparkContext}
     * @param numberOfFeatures Number of iterations
     * @param pointsWithLabelsPath Input path
     * @param alpha Learning rate
     * @param maxIterations Maximum number of iterations
     * @param outputPathTheta Output Path
     * @param labelPosition Position of labels in the CSV
     * @throws Exception
     */
    public static void runProgram(final JavaSparkContext sc,
                                  final int numberOfFeatures,
                                  final String pointsWithLabelsPath,
                                  float alpha,
                                  int maxIterations,
                                  final String outputPathTheta,
                                  int labelPosition) throws Exception {
        JavaRDD<Point> pointsWithLabels = parsePointsWithLabels(sc, pointsWithLabelsPath, numberOfFeatures, labelPosition).cache();

        // Calculate the total number of points present in the training set
        //@formatter:off
        @SuppressWarnings("serial")
        final Integer numberOfPoints = pointsWithLabels
                .map(new Function<Point, Integer>() {
                    @Override
                    public Integer call(Point p) throws Exception {
                        return 1;
                    }
                })
                .reduce(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                });
        //@formatter:on

        final Theta theta = new Theta(numberOfFeatures);

        // Main iteration: Do maxInteration iteration before the result is returned
        for (int i = 0; i < maxIterations; i++) {

            // Calculate the sum of the gradients
            //@formatter:off
            @SuppressWarnings("serial")
            Gradient sumGradient = pointsWithLabels
                    .map(new Function<Point, Gradient>() {
                        @Override
                        public Gradient call(Point pointWithLabel) throws Exception {
                            Gradient gradient = new Gradient(numberOfFeatures);
                            ArrayRealVector thetaVector = new ArrayRealVector(theta.getComponents(), false);
                            ArrayRealVector pointVector = new ArrayRealVector(pointWithLabel.getFeatures(), false);

                            for (int j = 0; j < gradient.getComponents().length; j++) {
                                gradient.setComponent(j,
                                        ((sigmoid(thetaVector.dotProduct(pointVector)) - pointWithLabel.getLabel()) * pointWithLabel.getFeature(j))
                                                / numberOfPoints);
                            }

                            return gradient;
                        }
                    })
                    .reduce(new Function2<Gradient, Gradient, Gradient>() {

                        @Override
                        public Gradient call(Gradient grad0, Gradient grad1) throws Exception {
                            // grad0 += grad1
                            for (int i = 0; i < grad0.getComponents().length; i++) {
                                grad0.setComponent(i, grad0.getComponent(i) + grad1.getComponent(i));
                            }

                            return grad0;
                        }

                    });
            //@formatter:on

            // Update theta using the gradient sum, the total number of points in the training set,
            // and alpha
            ArrayRealVector thetaVector = new ArrayRealVector(theta.getComponents(), false);
            ArrayRealVector gradientVector = new ArrayRealVector(sumGradient.getComponents(), false);
            theta.setComponents(thetaVector.subtract(gradientVector.mapMultiplyToSelf(alpha)).getData());

        }

        // Print the result theta after all iterations are done
        System.out.println("The calculated theta is:");
        StringBuilder sb = new StringBuilder();
        for (double d : theta.getComponents()) {
            System.out.println(d);
            sb.append(Double.toString(d));
            sb.append(',');
        }

        // Write the calculated theta to the given output path
        sb.deleteCharAt(sb.length() - 1);
        FileWriter fw = new FileWriter(outputPathTheta + "/0");
        fw.append(sb.toString());
        fw.close();

    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Utility methods ---------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * This method is used to parse an input text file into an DataSet of Points. Label position
     * specifies the position of the label value in the CSV file
     * 
     * @param sc Spark context provided as {@link JavaSparkContext}
     * @param pointsWithLabelsPath Input path
     * @param numberOfFeatures Number of iterations
     * @param labelPosition Position of labels in the CSV
     * @return DataSet of Points
     */
    @SuppressWarnings("serial")
    public static JavaRDD<Point> parsePointsWithLabels(JavaSparkContext sc,
                                                       String pointsWithLabelsPath,
                                                       final int numberOfFeatures,
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
                        if (a < numberOfFeatures && !"".equals(split[i].trim())) {
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

}
