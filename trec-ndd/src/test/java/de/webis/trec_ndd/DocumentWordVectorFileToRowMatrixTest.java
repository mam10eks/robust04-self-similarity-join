package de.webis.trec_ndd;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import scala.Tuple2;

public class DocumentWordVectorFileToRowMatrixTest extends SharedJavaSparkContext {
	@Test
	public void testSmallExampleWithThreeDocuments() {
		JavaRDD<Tuple2<Vector, Long>> actual = datasetA(jsc());
		JavaRDD<Tuple2<Vector, Long>> expected = vectorsWithIndizes(new double[] { 0.020, 0.1, -0.3 },
				new double[] { 0.010, 0.2, -0.2 }, new double[] { -0.010, -0.1, 0.2 });
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}

	@Test
	public void testSmallExampleWithFourDocuments() {
		RDD<String> input = inputB(jsc());
		JavaRDD<Tuple2<Vector, Long>> actual = SparkNearDuplicateDetection.documentVectorToRows(input).toJavaRDD();
		JavaRDD<Tuple2<Vector, Long>> expected = vectorsWithIndizes(new double[] { 1, 0 }, new double[] { 0, 1 }, new double[] { 1, 1 },
				new double[] { 0.99, 0.99 });
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}

	@Test
	@Ignore
	public void testExampleC() {
		Assert.fail("asdsad");
	}
	
	@Test
	@Ignore
	public void similaritiesBetwenAAndB() {
		RDD a = datasetA(jsc()).rdd();
		RDD b = datasetB(jsc()).rdd();
		
		RDD actualSimilarities = SparkNearDuplicateDetection.documentVectorToRowMatrix(a, b);
		
	}

	public static JavaRDD<Tuple2<Vector, Long>> datasetA(JavaSparkContext jsc) {
		return SparkNearDuplicateDetection.documentVectorToRows(inputA(jsc)).toJavaRDD();
	}
	
	public static RDD<String> inputA(JavaSparkContext jsc) {
		return input(jsc, "{\"content\": [0.020, 0.1, -0.3], \"docid\": \"a\"}",
				"{\"content\": [0.010, 0.2, -0.2], \"docid\": \"b\"}",
				"{\"content\": [-0.010, -0.1, 0.2], \"docid\": \"c\"}");
	}

	private JavaRDD<Tuple2<Vector, Long>> datasetB(JavaSparkContext jsc) {
		return SparkNearDuplicateDetection.documentVectorToRows(inputB(jsc))
				.toJavaRDD();
	}
	
	public static RDD<String> inputB(JavaSparkContext jsc) {
		return input(jsc,
				"{\"content\": [1, 0], \"docid\": \"a\"}",
				"{\"content\": [0, 1], \"docid\": \"b\"}",
				"{\"content\": [1, 1], \"docid\": \"c\"}",
				"{\"content\": [0.99, 0.99], \"docid\": \"d\"}");
	}
	
	public static RDD<String> inputC(JavaSparkContext jsc) {
		return input(jsc,
				"{\"content\": [0, 1, 2], \"docid\": \"a\"}",
				"{\"content\": [2, 1, 0], \"docid\": \"b\"}",
				"{\"content\": [2, 0, 1], \"docid\": \"c\"}",
				"{\"content\": [1, 0, 2], \"docid\": \"d\"}");
	}

	private static RDD<String> input(JavaSparkContext jsc, String... lines) {
		return jsc.parallelize(Arrays.asList(lines)).rdd();
	}
	
	private JavaRDD<Tuple2<Vector, Long>> vectorsWithIndizes(double[]... vectors) {
		return vectorsWithIndizes(jsc(), vectors);
	}
	
	public static JavaRDD<Tuple2<Vector, Long>> vectorsWithIndizes(JavaSparkContext jsc, double[]... vectors) {
		return vectors(jsc, vectors)
				.zipWithIndex().rdd().toJavaRDD();
	}
	
	public static JavaRDD<Vector> vectors(JavaSparkContext jsc, double[]... vectors) {
		return jsc.parallelize(Stream.of(vectors).map(vector -> Vectors.dense(vector)).collect(Collectors.toList()));
	}
}
