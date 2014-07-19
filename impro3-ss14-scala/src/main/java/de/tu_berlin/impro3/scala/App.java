package de.tu_berlin.impro3.scala;

import java.util.HashMap;

import de.tu_berlin.impro3.core.AppRunner;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.reflections.Reflections;

@SuppressWarnings("WeakerAccess")
public class App {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        AppRunner runner = new AppRunner("de.tu_berlin.impro3.scala", "impro3-ss14-scala", "Scala");
        runner.run(args);
    }
}
