package de.tu_berlin.impro3.scala;

import java.util.HashMap;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.reflections.Reflections;

@SuppressWarnings("WeakerAccess")
public class Application {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        ArgumentParser parser = ArgumentParsers.newArgumentParser("impro3-ss14-scala").defaultHelp(true).description("Run a ML algorithm on Scala");
        Subparsers subparsers = parser.addSubparsers().help("algorithm name");

        HashMap<String, Algorithm.Config> algorithmConfigs = new HashMap<>();

        Reflections reflections = new Reflections("de.tu_berlin.impro3.scala");

        for (Class<? extends Algorithm.Config> clazz : reflections.getSubTypesOf(Algorithm.Config.class)) {
            try {
                Algorithm.Config config = clazz.newInstance();
                config.setup(subparsers.addParser(config.CommandName(), true));
                algorithmConfigs.put(config.CommandName(), config);
            } catch (InstantiationException | IllegalAccessException e) {
                System.out.println(String.format("ERROR: Cannot instantiate algorithm class '%s'", clazz.getCanonicalName()));
                System.exit(1);
            }
        }

        try {
            Namespace ns = parser.parseArgs(args);

            // hack: command name is first arg from left to right that matches an algorithm key
            for (String arg : args) {
                if (algorithmConfigs.containsKey(arg)) {
                    algorithmConfigs.get(arg).instantiate(ns.getAttrs()).run();
                    System.exit(0);
                }
            }

            throw new RuntimeException("Unknown algorithm name");

        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected exception:");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
