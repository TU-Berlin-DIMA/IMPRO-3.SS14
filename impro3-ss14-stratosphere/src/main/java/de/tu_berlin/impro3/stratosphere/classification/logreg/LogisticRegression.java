package de.tu_berlin.impro3.stratosphere.classification.logreg;

import de.tu_berlin.impro3.core.Algorithm;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.CrossFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.configuration.Configuration;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

@SuppressWarnings("serial")
public class LogisticRegression extends Algorithm {

    final int numberOfFeatures;

    final int labelPosition;

    final int maxIterations;

    final float alpha;

    final String pointsWithLabelsPath;

    final String outputPathTheta;

    @SuppressWarnings("unused")
    public LogisticRegression(Namespace ns) {
        this(ns.getInt(Command.KEY_NUM_FEATURES),
             ns.getInt(Command.KEY_LABEL_POSITION),
             ns.getInt(Command.KEY_ITERATIONS),
             ns.getFloat(Command.KEY_ALPHA),
             ns.getString(Command.KEY_INPUT),
             ns.getString(Command.KEY_OUTPUT));
    }

    public LogisticRegression(final int numberOfFeatures,
                              final int labelPosition,
                              final int maxIterations,
                              float alpha,
                              final String pointsWithLabelsPath,
                              final String outputPathTheta) {
        this.numberOfFeatures = numberOfFeatures;
        this.labelPosition = labelPosition;
        this.maxIterations = maxIterations;
        this.alpha = alpha;
        this.pointsWithLabelsPath = pointsWithLabelsPath;
        this.outputPathTheta = outputPathTheta;
    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Algorithn Command -------------------------------------------
    // --------------------------------------------------------------------------------------------


    public static class Command extends Algorithm.Command<LogisticRegression> {

        /*
         * 
         * final int numberOfFeatures, String pointsWithLabelsPath, float alpha, int maxIterations,
         * String outputPathTheta, int labelPosition
         */

        public final static String KEY_NUM_FEATURES = "algorithm.logreg.num.features";

        public final static String KEY_LABEL_POSITION = "algorithm.logreg.label.position";

        public final static String KEY_ALPHA = "algorithm.logreg.alpha";

        public final static String KEY_ITERATIONS = "algorithm.logreg.num.iterations";

        public Command() {
            super("logreg", "Logistic Regression", LogisticRegression.class);
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
                    .metavar("LABEL-POSITION")
                    .help("Position of label in CSV");
            parser.addArgument("iterations") //  "-i", "--iterations"
                    .type(Integer.class)
                    .dest(KEY_ITERATIONS)
                    .metavar("NUM-ITERATIONS")
                    .help("Number of iterations");
            parser.addArgument("alpha") // "-a", "--alpha"
                    .type(Float.class)
                    .dest(KEY_ALPHA)
                    .metavar("ALPHA")
                    .help("Learning rate alpha");
            //@formatter:on

            super.setup(parser);
        }
    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Data Structures ---------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * This class represents a labeled point in space. It consists of a feature vector (represented
     * as array of doubles) and an Integer label.
     */
    public static class Point extends Tuple2<Integer, double[]> {

        /**
         * Returns the vector of feathres
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
         * Sets the feature vectore
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
    public static class Vector extends Tuple1<double[]> {

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
         * Returns the double vector that represents the components of this Vector
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
    public static class Gradient extends Vector {

        public Gradient() {
            // default constructor needed for instantiation during serialization
        }

        public Gradient(int size) {
            super(size);
        }
    }

    // --------------------------------------------------------------------------------------------
    // --------------------------------- Algorithm ------------------------------------------------
    // --------------------------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        doRun(numberOfFeatures, pointsWithLabelsPath, alpha, maxIterations, outputPathTheta, labelPosition);
    }

    /**
     * runs the logistic regression program with the given parameters
     * 
     * @param numberOfFeatures Number of iterations
     * @param pointsWithLabelsPath Input path
     * @param alpha Learning rate
     * @param maxIterations Maximum number of iterations
     * @param outputPathTheta Output Path
     * @param labelPosition Position of labels in the CSV
     * @throws Exception
     */
    private static void doRun(final int numberOfFeatures,
                              String pointsWithLabelsPath,
                              float alpha,
                              int maxIterations,
                              String outputPathTheta,
                              int labelPosition) throws Exception {

        Configuration config = new Configuration();
        config.setFloat("alpha", alpha);
        config.setInteger("numberOfFeatures", numberOfFeatures);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Point> pointsWithLabels = parsePointsWithLabels(env, pointsWithLabelsPath, numberOfFeatures, labelPosition);

        DataSet<Integer> numberOfPoints = pointsWithLabels.map(new MapFunction<Point, Integer>() {

            @Override
            public Integer map(Point t) throws Exception {
                return 1;
            }
        }).reduce(new ReduceFunction<Integer>() {

            @Override
            public Integer reduce(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        DataSet<Theta> theta = env.fromElements(new Theta(numberOfFeatures));

        IterativeDataSet<Theta> iteration = theta.iterate(maxIterations);

        DataSet<Gradient> gradient = pointsWithLabels.map(new MapFunction<Point, Gradient>() {

            private Gradient gradient;

            private Integer numberOfPoints;

            private double[] thetaVector;


            public void open(Configuration parameters) throws Exception {
                // load broadcast variable for number of points
                numberOfPoints = (Integer) getRuntimeContext().getBroadcastVariable("numberOfPoints").iterator().next();

                // create object for Gradient
                gradient = new Gradient(parameters.getInteger("numberOfFeatures", 0));

                if (getRuntimeContext().getBroadcastVariable("iteration").iterator().hasNext()) {
                    thetaVector = ((Theta) getRuntimeContext().getBroadcastVariable("iteration").iterator().next()).getComponents();
                }
            }

            /**
             * Builds the gradient of one point and divides it by numberOfPoints
             */
            @Override
            public Gradient map(Point pointWithLabel) throws Exception {

                double[] pointVector = pointWithLabel.getFeatures();

                for (int i = 0; i < gradient.getComponents().length; i++) {
                    gradient.setComponent(i,
                                          ((sigmoid(dotProduct(thetaVector, pointVector)) - pointWithLabel.getLabel()) * pointWithLabel.getFeature(i))
                                                  / numberOfPoints);
                }

                return gradient;
            }
        })
                                                     .withBroadcastSet(numberOfPoints, "numberOfPoints")
                                                     .withBroadcastSet(iteration, "iteration")
                                                     .withParameters(config);

        DataSet<Gradient> sumGradient = gradient.reduce(new ReduceFunction<Gradient>() {

            /**
             * Builds the sum to obtain full average gradient
             */
            @Override
            public Gradient reduce(Gradient gradient1, Gradient gradient2) throws Exception {
                // grad(i) +=
                for (int i = 0; i < gradient1.getComponents().length; i++) {
                    gradient1.setComponent(i, gradient1.getComponent(i) + gradient2.getComponent(i));
                }

                return gradient1;
            }
        });

        DataSet<Theta> modifiedTheta =
                sumGradient.crossWithTiny(iteration).with(new CrossFunction<LogisticRegression.Gradient, LogisticRegression.Theta, Theta>() {

                    private Float alpha;

                    private Theta modifiedTheta;

                    public void open(Configuration parameters) throws Exception {
                        modifiedTheta = new Theta(parameters.getInteger("numberOfFeatures", 0));
                        alpha = parameters.getFloat("alpha", 0.1f);
                    }

                    /**
                     * Sums up all gradients to get full average gradient
                     */
                    @Override
                    public Theta cross(Gradient gradient, Theta lastTheta) throws Exception {

                        modifiedTheta.setComponents(subtract(lastTheta.getComponents(), mapMultiplyToSelf(gradient.getComponents(), alpha)));

                        return modifiedTheta;
                    }
                }).withParameters(config);

        DataSet<Theta> resultTheta = iteration.closeWith(modifiedTheta);

        resultTheta.writeAsText(outputPathTheta);

        env.execute("Logistic Regression");

    }

    // --------------------------------------------------------------------------------------------
    // ------------------------------ Utility methods ---------------------------------------------
    // --------------------------------------------------------------------------------------------

    /**
     * This class is used to parse an input text file into an DataSet of Points. Label position
     * specifies the position of the label value in the CSV file
     * 
     * @return DataSet of Points
     */
    public static DataSet<Point> parsePointsWithLabels(ExecutionEnvironment env,
                                                       String pointsWithLabelsPath,
                                                       final int numberOfFeatures,
                                                       final int labelPosition) {
        return env.readTextFile(pointsWithLabelsPath).map(new MapFunction<String, Point>() {

            /**
             * Parses the input files into Point objects
             */
            @Override
            public Point map(String value) throws Exception {

                Point p = new Point();

                String[] split = value.split(",");
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
     * @param a
     * @param b
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
     * @param a
     * @param b
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
