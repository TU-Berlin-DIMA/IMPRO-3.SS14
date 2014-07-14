package de.tu_berlin.impro3.spark;

import de.tu_berlin.impro3.core.AppRunner;

public class App {

    public static void main(String[] args) {
        AppRunner runner = new AppRunner("de.tu_berlin.impro3.spark", "impro3-ss14-spark", "Spark");
        runner.run(args);
    }
}
