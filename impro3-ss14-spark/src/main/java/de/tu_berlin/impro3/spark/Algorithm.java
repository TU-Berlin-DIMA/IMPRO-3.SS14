package de.tu_berlin.impro3.spark;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.lang.reflect.InvocationTargetException;

public abstract class Algorithm {

    /**
     * An abstract command cline interface for algorithms. Each algorithm should define its
     * corresponding command.
     * 
     * @param <A>
     */
    public static abstract class Command<A extends Algorithm> {

        // argument names
        public static final String KEY_INPUT = "algorithm.input";

        public static final String KEY_OUTPUT = "algorithm.output";

        public final String name;

        public final String help;

        public final Class<A> clazz;

        protected Command(String name, String help, Class<A> clazz) {
            this.name = name;
            this.help = help;
            this.clazz = clazz;
        }

        /**
         * Configures the Argparse4j subparser for this command.
         * 
         * @param parser The subparser for this command.
         */
        public void setup(Subparser parser) {
            //@formatter:off
            // default options
            parser.addArgument("-?")
                  .action(Arguments.help())
                  .help("show this help message and exit")
                  .setDefault(Arguments.SUPPRESS);
            // input and output path
            parser.addArgument("input")
                  .type(String.class)
                  .dest(KEY_INPUT)
                  .metavar("INPUT")
                  .help("input file path");
            parser.addArgument("output")
                  .type(String.class)
                  .dest(KEY_OUTPUT)
                  .metavar("OUTPUT")
                  .help("output file path");
            //@formatter:on
        }

        /**
         * Create an instance of the algorithm.
         * 
         * @return A new algorithm instance.
         */
        public final A instantiate() throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
            return clazz.getConstructor().newInstance();
        }
    }

    abstract protected void run(Namespace ns) throws Exception;
}
