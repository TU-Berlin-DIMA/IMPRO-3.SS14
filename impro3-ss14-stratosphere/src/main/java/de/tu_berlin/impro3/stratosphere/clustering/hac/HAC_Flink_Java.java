package de.tu_berlin.impro3.stratosphere.clustering.hac;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.*;
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Scanner;

@SuppressWarnings("serial")
public class HAC_Flink_Java {

	public static enum LinkageMode { SINGLE, COMPLETE }

	/**
	This class represents one DataPoint from the "Bag of Words" input file. Basically, one DataPoint represents one line of the input file.
	Each DataPoint has a document ID, a term ID and the count of the term in the document.
	 */
	public static class DataPoint extends Tuple3<Integer, Integer, Integer> {

		public DataPoint() {
		}

		public DataPoint(Integer docid, Integer termid, Integer count) {
			this.f0 = docid;
			this.f1 = termid;
			this.f2 = count;
		}

        public Integer getDocID() {
            return this.f0;
        }


        public Integer getCount() { return this.f2; }
	}

	/**
	This class represents a document from the "Bag of Words". There are two fields, a document ID that identifies  the document and
	a cluster ID that identifies the cluster the document currently resides in. In the beginning,  the cluster ID is always equal to the
	document ID.
	 */
    public static class Document extends Tuple2<Integer, Integer> {

        public Document() {}

        public Document(Integer docid) {
            this.f0 = docid;
            this.f1 = docid;
        }

        public Integer getDocID() { return this.f1; }

        public void setClusterID(Integer id) {
            this.f0 = id;
        }

        public Integer getClusterID() {
            return this.f0;
        }
    }

	/**
	A ClusterPair contains two cluster IDs and the similarity of those clusters.
	 */
    public static class ClusterPair extends Tuple3<Double, Integer, Integer> implements Cloneable {

		public ClusterPair() {
		}

		@Override
		public ClusterPair clone() {
			return new ClusterPair(new Double(this.f0.doubleValue()), new Integer(this.f1.intValue()), new Integer(this.f2.intValue()));
		}

		public ClusterPair(Double sim, Integer cid0, Integer cid1) {
			this.f0 = sim;
			this.f1 = cid0;
			this.f2 = cid1;
		}

		public Double getSimilarity() {
			return this.f0;
		}

		public void setSimilarity(Double distance) {
			this.f0 = distance;
		}

		public Integer getCluster1() {
			return this.f1;
		}

		public Integer getCluster2() {
			return this.f2;
		}

		public void setCluster1(Integer clusterID) {
			this.f1 = clusterID;
		}

		public void setCluster2(Integer clusterID) {
			this.f2 = clusterID;
		}
	}

	/**
	This reducer finds the ClusterPair with the lowest similarity value. This is useful
	 for the COMPLETE_LINKAGE linkage option.
	 */
	public static class MinSimilarityReducer extends ReduceFunction<ClusterPair> {
		@Override
		public ClusterPair reduce(ClusterPair value1, ClusterPair value2) throws Exception {
			if (value1.getSimilarity().doubleValue() < value2.getSimilarity().doubleValue()) {
				return value1.clone();
			}
			return value2.clone();
		}
	}

	/**
	This reducer finds the ClusterPair with the highest similarity value. This is useful
	 for the SINGLE_LINKAGE linkage option.
	 */
	public static class MaxSimilarityReducer extends ReduceFunction<ClusterPair> {
		@Override
		public ClusterPair reduce(ClusterPair value1, ClusterPair value2) throws Exception {
			if (value1.getSimilarity().doubleValue() > value2.getSimilarity().doubleValue()) {
				return value1.clone();
			}
			return value2.clone();
		}
	}

	/**
	This FlatMap gets the ClusterPair with the minimal or maximal similarity (according to the linkage option) as a
	broadcast variable.	The Cluster with the lower ClusterID of this broadcast ClusterPair needs to be replaced by the
	higher cluster. Therefore, this flatmap searches for ClusterPairs where cluster1 or cluster2 are equal to cluster1 of the
	broadcast variable. If such a pair is found, the clusterID is replaced.
	 */
	public static class ClusterPairUpdater extends FlatMapFunction<ClusterPair, ClusterPair> {
		private ClusterPair mergedPair;
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			Collection<ClusterPair> min = this.getRuntimeContext().getBroadcastVariable("mergedPair");
			Iterator<ClusterPair> it = min.iterator();
			if (it.hasNext()) {
				this.mergedPair = min.iterator().next();
			} else {
				this.mergedPair = null;
			}
		}

		@Override
		public void flatMap(ClusterPair value, Collector<ClusterPair> out) throws Exception {
			if (mergedPair == null) {
				out.collect(value);
				return;
			}

			//if broadcast clusterID is found, it needs to be replaced(merged)
			if (value.getCluster1().equals(mergedPair.getCluster1())) {
				value.setCluster1(mergedPair.getCluster2());
			} else if (value.getCluster2().equals(mergedPair.getCluster1())) {
				value.setCluster2(mergedPair.getCluster2());
			}

			//Cluster with the smaller ID should always be in first place.
			if (value.getCluster1().intValue() < value.getCluster2().intValue()) {
				out.collect(value);
			} else if (value.getCluster1().intValue() > value.getCluster2().intValue()) {
				// swap cluster ID's:
				out.collect(new ClusterPair(value.getSimilarity(), value.getCluster2(), value.getCluster1()));
			}
		}
	}

	/**
	This class represents the change that the clusters have gone through. This is necessary because it is not yet possible to
	output data after every iteration. This way, we can keep track of the Clusters in each iteration of the algorithm.
	 */
    public static class ClusterHistory extends Tuple2<Integer, String> {
        public ClusterHistory() {
            f0 = 0;
            f1 = new String("");
        }

        public ClusterHistory(Integer oldid, Integer newid) {
            f0 = 0;
            f1 = new String("[" + oldid + "; " + newid + "]");
        }

        public void add(ClusterHistory newend) {
            f1 += newend.f1;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(f1);
            return builder.toString();
        }
    }

	/**
	This Reducer creates all the Documents and inserts them into a initial cluster.
	 */
	public static class ClusterAssignReducer extends GroupReduceFunction<DataPoint, Document> {
		@Override
		public void reduce(Iterator<DataPoint> values, Collector<Document> out) throws Exception {
			Document doc = new Document(values.next().getDocID());

			System.out.println("Inserting document " + doc.getDocID() + " into cluster " + doc.getClusterID());

			out.collect(doc);
		}
	}

	/**
	Joins two ClusterHistories together.
	 */
	public static class HistoryJoiner extends JoinFunction<ClusterHistory, ClusterHistory, ClusterHistory> {
		@Override
		public ClusterHistory join(ClusterHistory clusterHistory, ClusterHistory clusterHistory2) throws Exception {
			clusterHistory.add(clusterHistory2);
			return clusterHistory;
		}
	}

	/**
	 * Outputs the product of two terms.
	 */
	public static class SimilarityMap extends FlatMapFunction<Tuple2<DataPoint,DataPoint>, ClusterPair> {
		@Override
		public void flatMap(Tuple2<DataPoint, DataPoint> in, Collector<ClusterPair> out) throws Exception {
			if (in.f0.getDocID() < in.f1.getDocID()) {
				out.collect(new ClusterPair((double)in.f0.getCount()*in.f1.getCount(), in.f0.getDocID(), in.f1.getDocID()));
			}
		}
	}

	public static class HistoryUpdater extends MapFunction<ClusterPair, ClusterHistory> {
		@Override
		public ClusterHistory map(ClusterPair clusterPair) throws Exception {
			return new ClusterHistory(clusterPair.getCluster1(), clusterPair.getCluster2());
		}
	}

    public static void clusterDocuments(ExecutionEnvironment env, String linkage, DataSet<ClusterPair> similarities, String outputfile) {
        		/*
		Initializing the ClusterHistory
		 */
        ArrayList<ClusterHistory> startList = new ArrayList<>();
        startList.add(new ClusterHistory());

        DataSet<ClusterHistory> history = env.fromCollection(startList);

		/*
		Start of the iteration.
		 */
        DeltaIteration<ClusterHistory, ClusterPair> iteration = history.iterateDelta(similarities, 10000, 0);

		/*
		Differentiate between the linkage modes. In SINGLE link mode, the highest similarity symbolizes the shorted "distance" between two clusters.
		COMPLETE link needs the minimum similarity.
		 */
        ReduceFunction<ClusterPair> linkageReducer;
        if (linkage.equals("COMPLETE")) {
            linkageReducer = new MinSimilarityReducer();
        } else if (linkage.equals("SINGLE")) {
            linkageReducer = new MaxSimilarityReducer();
        } else {
            throw new RuntimeException("Illegal Linkage Option.");
        }

        DataSet<ClusterPair> linkageSet = iteration.getWorkset().reduce(linkageReducer);

        // just for printing / debug purposes
        /*
	    linkageSet = linkageSet.map(new MapFunction<ClusterPair, ClusterPair>() {
            @Override
            public ClusterPair map(ClusterPair value) throws Exception {
                System.out.println("value = " + value);
                return value;
            }
        });
	    */

        //update the ClusterHistory
        DataSet<ClusterHistory> update = linkageSet.map(new HistoryUpdater());

        //join History
        DataSet<ClusterHistory> delta = iteration.getSolutionSet().join(update).where(0).equalTo(0).with(new HistoryJoiner());

		/*
		The ClusterPairs are updated and duplicate ClusterPairs are removed.
		 */
        DataSet<ClusterPair> feedback = iteration.getWorkset()
                .flatMap(new ClusterPairUpdater()).withBroadcastSet(linkageSet, "mergedPair")
                .groupBy(1, 2).reduce(linkageReducer);

        DataSet<ClusterHistory> result = iteration.closeWith(delta, feedback);

        if(!outputfile.isEmpty())
            result.writeAsText(outputfile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        else
            result.print();
    }

	/**
	 * @param args [inputFile] [linkage(COMPLETE|SINGLE)] [outputFile] [prettyPrint(true|false)]
	 */
	public static void main(String[] args) {
		String filePath = null;
		try {
			filePath = new File("./impro3-ss14-stratosphere/src/main/resources/datasets/clustering/bag-of-words/docword.kos.txt.mini").getCanonicalPath();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String linkage = "COMPLETE";
		String outputPath = "/tmp/hac";
		boolean prettyPrint = false;

		// optional arguments:
		if (args.length > 0) {
			filePath = args[0];
			System.out.println("Input File: "  + filePath);
		}
		if (args.length > 1) {
			linkage = args[1];
			System.out.println("Linkage: " + linkage);
		}
		if (args.length > 2) {
			outputPath = args[2];
			System.out.println("Output File: " + outputPath);
		}
		if (args.length > 3) {
			prettyPrint = Boolean.parseBoolean(args[2]);
			System.out.println("PrettyPrint: " + prettyPrint);
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		Initial points are read from the file. Layout: docID termID termCount (separated by space).
		 */
		DataSet<DataPoint> points = env.readCsvFile(filePath).fieldDelimiter(' ').tupleType(DataPoint.class);

        DataSet<Document> documents = points.groupBy(0).reduceGroup(new ClusterAssignReducer());

		/*
		Join the Points on their TermID. Then multiply the counts of the two terms and save them as similarity in a ClusterPair, together with the two document IDs.
		By grouping on the two document ids, we get the multiplied count of all terms that appear in both documents. By computing the sum of the multiplied counts we
		get our similarity measure. This way, only terms that are present in both documents are considered and terms that appear often in both gain additional weight.
		 */
		DataSet<ClusterPair> similarities = points.join(points).where(1).equalTo(1).flatMap(new SimilarityMap()).groupBy(1,2).aggregate(Aggregations.SUM, 0);
        clusterDocuments(env, linkage, similarities, outputPath);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

		/*
		Pretty Printing. If the prettyPrint option is set, a new job is started that reformats the history to extract the clusterPairs for each iteration.
		This has to be done because it is not possible to write out data after each iteration. If the option is set, one file with the name of
		outputFile_iterationnumber is created.
		 */
		if (prettyPrint) {
			Scanner scanner = null;
			StringBuilder builder = new StringBuilder();

			try {
				scanner = new Scanner(new File(outputPath));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			while (scanner.hasNextLine()) {
				builder.append(scanner.nextLine());
			}

			OutputFormatter.formatAndWriteString(builder.toString(), outputPath + "_iterations");
		}
	}
}
