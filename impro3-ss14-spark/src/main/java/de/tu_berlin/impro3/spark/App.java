package de.tu_berlin.impro3.spark;

import java.util.Arrays;
import java.util.HashMap;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.reflections.Reflections;

@SuppressWarnings("WeakerAccess")
public class App {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        HashMap<String, Algorithm.Command> commands = new HashMap<>();

        Reflections.log = null;
        Reflections reflections = new Reflections("de.tu_berlin.impro3.spark");

        for (Class<? extends Algorithm.Command> clazz : reflections.getSubTypesOf(Algorithm.Command.class)) {
            try {
                Algorithm.Command command = clazz.newInstance();
                commands.put(command.name, command);
            } catch (InstantiationException | IllegalAccessException e) {
                System.out.println(String.format("ERROR: Cannot instantiate algorithm class '%s'", clazz.getCanonicalName()));
                System.exit(1);
            }
        }

        // construct argument parser
        //@formatter:off
        ArgumentParser parser = ArgumentParsers.newArgumentParser("impro3-ss14-spark", false)
                .defaultHelp(true)
                .description("Run a ML algorithm on Spark");
        parser.addSubparsers()
                .help("an algorithm to run")
                .dest("algorithm.name")
                .metavar("ALGORITHM");
        parser.addArgument("-?")
                .action(Arguments.help())
                .help("show this help message and exit")
                .setDefault(Arguments.SUPPRESS);
        //@formatter:on

        // register command arguments with the arguments parser (in order of command names)
        String[] commandKeys = commands.keySet().toArray(new String[commands.size()]);
        Arrays.sort(commandKeys);
        for (String key : commandKeys) {
            Algorithm.Command c = commands.get(key);
            c.setup(parser.addSubparsers().addParser(c.name, false).help(c.help));
        }

        try {
            Namespace ns = parser.parseArgs(args);

            String algorithmName = ns.getString("algorithm.name");

            if (algorithmName == null) {
                parser.printHelp();
                System.exit(0);
            }

            if (!commands.containsKey(algorithmName)) {
                throw new IllegalArgumentException("Illegal algorithm name");
            }

            commands.get(algorithmName).instantiate().run(ns);

        } catch (HelpScreenException e) {
            parser.handleError(e);
            System.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected exception:");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
