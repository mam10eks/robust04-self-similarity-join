package de.webis.trec_ndd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.nn.algorithm.CosineSignRandomProjectionNNS;
import com.linkedin.nn.model.CosineSignRandomProjectionModel;
import com.linkedin.nn.model.LSHNearestNeighborSearchModel;

import scala.Tuple2;
import scala.Tuple3;

public class SparkNearDuplicateDetection {
	private static final double THRESHOLD = 0.75;
	
	private static final String DIRECTORY = "/user/kibi9872/";
	
	public static void main(String[] args) {
		JavaSparkContext sc = sparkContext();
		RDD<String> input = sc.textFile(DIRECTORY + "document-vectors.jsonl").rdd();
		RDD<Tuple3<Long, Long, Double>> similarities = RowMatrixNearDuplicateDetection.similarColumns(input, 0.8);
		
		similarities.saveAsTextFile(DIRECTORY + "ndd-similarities.txt");
	}
	
	static JavaRDD<Tuple2<Long, Long>> similarEntries(RowMatrix matrix) {
		return matrix.columnSimilarities(THRESHOLD)
				.entries()
				.toJavaRDD()
				.filter(m -> m.value() >= THRESHOLD)
				.map(i -> new Tuple2<>(i.i(), i.j()));
	}

	static RDD<Tuple2<Vector, Long>> documentVectorToRows(RDD<String> input) {
		return input.zipWithIndex().toJavaRDD()
				.map(i -> new Tuple2<Vector, Long>(documentLineToVector(i._1), (Long) i._2)).rdd();
		
	}
	
	static RDD documentVectorToRowMatrix(RDD srcItems, RDD candidatePool) {
		int numFeatures = ((Vector) ((Tuple2)srcItems.first())._1).size();
		
		
		
		LSHNearestNeighborSearchModel<CosineSignRandomProjectionModel> model = new CosineSignRandomProjectionNNS("uuid")
	        .setNumHashes(300)
	        .setSignatureLength(15)
	        .setJoinParallelism(5000)
	        .setBucketLimit(1000)
	        .setShouldSampleBuckets(true)
	        .setNumOutputPartitions(100)
	        .createModel(numFeatures);

		return model.getAllNearestNeighbors(srcItems, candidatePool, 100);
	}

	@SuppressWarnings("unchecked")
	static Vector documentLineToVector(String document) {
		try {
			List<Number> d = (List<Number>) new ObjectMapper().readValue(document, Map.class).get("content");
			double[] dd = new double[d.size()];
			for(int i=0; i< dd.length; i++) {
				dd[i] = d.get(i).doubleValue();
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
