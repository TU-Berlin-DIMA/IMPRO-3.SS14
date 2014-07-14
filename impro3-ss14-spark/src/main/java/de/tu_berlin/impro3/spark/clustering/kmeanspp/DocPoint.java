package de.tu_berlin.impro3.spark.clustering.kmeanspp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


/**
 * Representing a data point from the Bag Of Words test case
 * 
 * A data point is a documents which contains a document ID, a set of words and their corresponding
 * frequencies
 * 
 */
public class DocPoint implements Serializable {

    private static final long serialVersionUID = 6917201675779889484L;

    public int docId = -1;

    /* map key is the id of word, while value indicates the count */
    public HashMap<Integer, Double> words = new HashMap<Integer, Double>();

    public DocPoint() {

    }

    public DocPoint(int id) {
        this.docId = id;
    }

    public DocPoint(DocPoint other) {
        this.docId = other.docId;
        this.words.putAll(other.words);
    }

    public void add(DocPoint other) {

        for (Map.Entry<Integer, Double> itr : other.words.entrySet()) {
            Double v1 = this.words.get(itr.getKey());
            if (v1 == null) {
                this.words.put(itr.getKey(), itr.getValue());
            } else {
                this.words.put(itr.getKey(), itr.getValue() + v1);
            }
        }
    }

    public static DocPoint add(DocPoint p1, DocPoint p2) {

        DocPoint result = new DocPoint();

        for (Map.Entry<Integer, Double> itr : p2.words.entrySet()) {
            Double v1 = p1.words.get(itr.getKey());
            if (v1 == null) {
                result.words.put(itr.getKey(), itr.getValue());
            } else {
                result.words.put(itr.getKey(), itr.getValue() + v1);
            }
        }
        return result;
    }

    public void div(long val) {
        for (int key : this.words.keySet()) {
            Double value = this.words.get(key);
            if (value != null) {
                this.words.put(key, value / val);
            }
        }
    }

    public static DocPoint div(DocPoint p, long val) {
        DocPoint result = new DocPoint(p.docId);

        for (Map.Entry<Integer, Double> itr : p.words.entrySet()) {
            result.words.put(itr.getKey(), itr.getValue() / val);
        }
        return result;
    }

    public double distanceSquare(DocPoint other) {

        double result = 0;
        HashSet<Integer> intersection = new HashSet<Integer>();

        for (Map.Entry<Integer, Double> itr : other.words.entrySet()) {
            Double v1 = this.words.get(itr.getKey());
            double v2 = itr.getValue();
            if (v1 == null) {
                result = v2 * v2;
            } else {
                result += (v2 - v1) * (v2 - v1);
                intersection.add(itr.getKey());
            }
        }

        for (Map.Entry<Integer, Double> itr : this.words.entrySet()) {
            if (!intersection.contains(itr.getKey())) {
                result += itr.getValue() * itr.getValue();
            }
        }

        return result;
    }

    public static double distanceSquare(DocPoint p1, DocPoint p2) {

        double result = 0;
        HashSet<Integer> intersection = new HashSet<Integer>();

        for (Map.Entry<Integer, Double> itr : p2.words.entrySet()) {
            Double v1 = p1.words.get(itr.getKey());
            double v2 = itr.getValue();
            if (v1 == null) {
                result = v2 * v2;
            } else {
                result += (v2 - v1) * (v2 - v1);
                intersection.add(itr.getKey());
            }
        }

        for (Map.Entry<Integer, Double> itr : p1.words.entrySet()) {
            if (!intersection.contains(itr.getKey())) {
                result += itr.getValue() * itr.getValue();
            }
        }

        return result;
    }

    @Override
    public String toString() {
        return "" + docId;
        /*
         * String result = "docId:"+this.docId; for(Map.Entry<Integer, Double> itr :
         * this.words.entrySet()) { result += ", " + itr.getKey() + "[" + itr.getValue() + "]"; }
         * return result;
         */
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        if (!(obj instanceof DocPoint))
            return false;

        DocPoint other = (DocPoint) obj;

        return this.docId == other.docId && this.words.equals(other.words);
    }

    @Override
    public int hashCode() {
        int prime = 37;
        int result = 1;

        result = result * prime + this.docId;
        result = result * prime + this.words.hashCode();

        return result;
    }
}
