IMPRO-3 (SS14)
==============

This project contains a set of machine learning algorithms implemented on top of [Scala](http://scala-lang.org/), [Stratosphere](http://stratosphere.eu/), and [Spark](http://spark-project.org/) by master students attending the "Big Data Analytics Project" at [FG DIMA](http://www.dima.tu-berlin.de/), TU Berlin in the 2014 spring term.


Instructions for Contributors
-----------------------------

Master students conrtibuting to the project should follow the instructions below:

### 1. Clone the code with Git

Use your group repository as «origin» and the main repository as «upstream»:

``` bash
export GROUP=GXX # configure your group number, e.g. G07
git clone git@github.com:TU-Berlin-DIMA/IMPRO-3.SS14.${GROUP}.git
cd IMPRO-3.SS14.${GROUP}
git remote add upstream git@github.com:TU-Berlin-DIMA/IMPRO-3.SS14.git
git fetch upstream
```

Setup and push an appropriate branch structure (this should be done only once per group):

``` bash
git checkout -b dev_scala
git checkout -b dev_stratosphere
git checkout -b dev_spark
git push origin dev_scala
git push origin dev_stratosphere
git push origin dev_spark
```

Each of the other group members can then merely checkout the existing branches:

``` bash
git checkout -b dev_scala origin/dev_scala
git checkout -b dev_stratosphere origin/dev_stratosphere
git checkout -b dev_spark origin/dev_spark
```

### 2. Import the project into your IDE

We recommend using either [IntelliJ](http://www.jetbrains.com/idea/) or [Eclipse](http://eclipse.org/). To enable auto-completion and syntax highlighting for the Scala code in your project, make sure you have the appropriate Scala plugin installed.

Project dependencies and build lifecycle are configured via [Maven](http://maven.apache.org/), so the easiest way to setup the project in your IDE is to point the Maven importer to the local Git clone location.

### 3. Contribute code

In the course of the spring term, each group should provide unit-tested implementations of one machine learning algorithm for [Scala](http://scala-lang.org/), [Stratosphere](http://stratosphere.eu/), and [Spark](http://spark-project.org/).

When you develop your code, please follow the workflow below:

  1.  Collaborate within the group. Create small commits into the `dev_{system}` branches and exchange them push/pull to `«origin»/dev_{system}`.
  1.  Make sure you frequently pull and rebase `«upstream»/master` onto the `dev_{system}` branch.
  1.  Once the algorithm is unit-tested and works, [squash all small commits](http://gitready.com/advanced/2009/02/10/squashing-commits-with-rebase.html) from the `dev_{system}` branch into one or two commits (e.g. one for the algorithm and one for the uni-test) and push them into `«origin»/dev_{system}`.
  1.  [Create a pull request](https://help.github.com/articles/creating-a-pull-request) from `«origin»/dev_{system}` to `«upstream»/master`.
  1.  If everything is fine, we will merge your code into `«upstream»/master`. You can then pull from `«upstream»/master` and push the merged version into `«origin»/master`.

### 4. Contribute your project presentations

Each group should also prepare and update an algorithm presentation for their particular algorithm. Please contribute updates to your slide sets using pull requests.

The current set of presentations can be found below:

  1.  [Introduction](doc/slides/01_Introduction.pdf)
  1.  [Scrum Introduction](doc/slides/02_Scrum_Introduction.pdf)
  1.  [Scala Introduction](doc/slides/03_Scala_Introduction.pdf)
  1.  Algorithm Presentations
      1.  [Logistic Regression](doc/slides/04_1_LogReg.pdf)
      1.  [Random Forest](doc/slides/04_2_RandomForest.pdf)
      1.  [Canopy Clustering](doc/slides/04_3_Canopy.pdf)
      1.  [Hierarchical Agglomerative Clustering](doc/slides/04_4_HAC.pdf)
      1.  [K-Means++](doc/slides/04_5_KMeansPlusPlus.pdf)




