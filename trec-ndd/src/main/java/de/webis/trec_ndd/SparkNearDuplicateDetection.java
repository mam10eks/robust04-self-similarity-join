package de.webis.trec_ndd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkNearDuplicateDetection {
	public static void main(String[] args) {
		JavaSparkContext sc = sparkContext();
		RowMatrix a = documentVectorToRowMatrix(sc.textFile(""));
		RowMatrix b = documentVectorToRowMatrix(sc.textFile(""));
//		
//		a.
	}

	static RowMatrix documentVectorToRowMatrix(JavaRDD<String> input) {
		JavaRDD<Vector> rows = input.map(SparkNearDuplicateDetection::documentLineToVector);

		return new RowMatrix(rows.rdd());
	}

	@SuppressWarnings("unchecked")
	private static Vector documentLineToVector(String document) {
		try {
			List<Double> d = (List<Double>) new ObjectMapper().readValue(document, Map.class).get("content");
			double[] dd = new double[d.size()];
			for(int i=0; i< dd.length; i++) {
				dd[i] = d.get(i);
			}
			
			return Vectors.dense(dd);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static JavaSparkContext sparkContext() {
		SparkConf config = new SparkConf().setAppName("Trec-Near-Duplicate-Detection");

		return new JavaSparkContext(config);
	}
}
