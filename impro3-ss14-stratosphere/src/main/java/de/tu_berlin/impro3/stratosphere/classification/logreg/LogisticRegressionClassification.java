package de.tu_berlin.impro3.stratosphere.classification.logreg;

import java.util.Iterator;

import de.tu_berlin.impro3.core.Algorithm;
import de.tu_berlin.impro3.stratosphere.classification.logreg.LogisticRegression.Point;
import de.tu_berlin.impro3.stratosphere.classification.logreg.LogisticRegression.Theta;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.util.Collector;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * 
 * This class is used for classification of data with an previously obtained theta
 * 
 */
@SuppressWarnings("serial")
public class LogisticRegressionClassification extends Algorithm {

    final int numberOfFeatures;

    final int labelPosition;

    final String pointsWithLabelsPath;

    final String outputPath;

    final String outputPathTheta;

    @SuppressWarnings("unused")
    public LogisticRegressionClassification(Namespace ns) {
        this(ns.getInt(Command.KEY_NUM_FEATURES),
             ns.getInt(Command.KEY_LABEL_POSITION),
             ns.getString(Command.KEY_INPUT),
             ns.getString(Command.KEY_OUTPUT),
             ns.getString(Command.KEY_OUTPUT_THETA));
    }

    public LogisticRegressionClassification(final int numberOfFeatures,
                                            final int labelPosition,
                                            final String pointsWithLabelsPath,
                                            final String outputPath,
                                            final String outputPathTheta) {
        this.numberOfFeatures = numberOfFeatures;
        this.labelPosition = labelPosition;
        this.pointsWithLabelsPath = pointsWithLabelsPath;
        this.outputPathTheta = outputPathTheta;
        this.outputPath = outputPath;
    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Algorithn Command -------------------------------------------
    // --------------------------------------------------------------------------------------------


    public static class Command extends Algorithm.Command<LogisticRegressionClassification> {

        /*
         * 
         * final int numberOfFeatures, String pointsWithLabelsPath, float alpha, int maxIterations,
         * String outputPathTheta, int labelPosition
         */

        public final static String KEY_NUM_FEATURES = "algorithm.logreg.num.features";

        public final static String KEY_ITERATIONS = "algorithm.logreg.num.iterations";

        public final static String KEY_LABEL_POSITION = "algorithm.logreg.label.position";

        public static final String KEY_OUTPUT_THETA = "algorithm.output.theta";

        public Command() {
            super("logreg", "Logistic Regression", LogisticRegressionClassification.class);
        }

        @Override
        public void setup(Subparser parser) {
            //@formatter:off
            parser.addArgument("features") // "-f", "--features"
                    .type(Integer.class)
                    .dest(KEY_NUM_FEATURES)
                    .metavar("NUM-FEATURES")
                    .help("Number of features");
            parser.addArgument("label-position") //  "-l", "--label-position"
                    .type(Integer.class)
                    .dest(KEY_LABEL_POSITION)
                    .metavar("LABEL-POSITION");
            parser.addArgument("iterations") //  "-i", "--iterations"
                    .type(Integer.class)
                    .dest(KEY_ITERATIONS)
                    .metavar("NUM-ITERATIONS")
                    .help("Number of iterations");
            parser.addArgument("theta")
                    .type(String.class)
                    .dest(KEY_OUTPUT_THETA)
                    .metavar("THETA-PATH")
                    .help("output file path (theta)");
            //@formatter:on

            super.setup(parser);
        }
    }

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
            this.f0 = classification;
        }

        public double getClassification() {
            return this.f0;
        }
    }

    // --------------------------------------------------------------------------------------------
    // --------------------------------- Algorithm ------------------------------------------------
    // --------------------------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        runProgram(numberOfFeatures, pointsWithLabelsPath, outputPathTheta, outputPath, labelPosition);
    }

    /**
     * runs the logist regression classification program with the given parameters
     * 
     * @param thetaPath Classification vector path
     * @param numberOfFeatures Number of iterations
     * @param pointsWithLabelsPath Input path
     * @param labelPosition Position of labels in the CSV
     * @throws Exception
     */
    private static void runProgram(int numberOfFeatures, String pointsWithLabelsPath, String thetaPath, String outputPath, int labelPosition) throws Exception {

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

                for (int i = 0; i < split.length; i++) {
                    features[i] = Double.parseDouble(split[i].replaceAll("[^0-9.,-]+", ""));
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

                double classification =
                        LogisticRegression.sigmoid(LogisticRegression.dotProduct(theta.getComponents(), pointWithLabel.getFeatures()));

                point.setClassification(classification);
                point.setPoint(pointWithLabel);

                return point;
            }
        }).withBroadcastSet(theta, "theta");

        classification.writeAsCsv(outputPath);

        DataSet<Integer> numberOfCorrectClassifications = classification.reduceGroup(new GroupReduceFunction<ClassifiedPoint, Integer>() {

            /**
             * Builds the sum of all correctly classified points
             */
            @Override
            public void reduce(Iterator<ClassifiedPoint> values, Collector<Integer> out) throws Exception {

                int cnt = 0;
                while (values.hasNext()) {

                    ClassifiedPoint p = values.next();

                    if (p.getPoint().getLabel() == 0) {
                        if (p.getClassification() < 0.5) {
                            cnt++;
                        }
                    } else {
                        if (p.getClassification() >= 0.5) {
                            cnt++;
                        }
                    }
                }

                out.collect(cnt);
            }
        });

        numberOfCorrectClassifications.print();

        env.execute("Logistic Regression");

        System.out.println("= Number of correct classified points");

    }
}
