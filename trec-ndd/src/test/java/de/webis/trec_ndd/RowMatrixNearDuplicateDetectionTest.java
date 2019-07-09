package de.webis.trec_ndd;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.rdd.RDD;
import org.junit.Test;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

public class RowMatrixNearDuplicateDetectionTest extends SharedJavaSparkContext {
	@Test
	public void testRowMatrixForA() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputA(jsc());
		
		JavaRDD<Vector> expected = DocumentWordVectorFileToRowMatrixTest.vectors(jsc(),
				new double[] { 0.020, 0.01, -0.010 },
				new double[] { 0.1, 0.2, -0.1 },
				new double[] { -0.3, -0.2, 0.2 });
		
		JavaRDD<Vector> actual = RowMatrixNearDuplicateDetection.asRowTransposedMatrix(input.toJavaRDD()).rows().toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}
	
	@Test
	public void testSelfSimililaritiesForAWithThresholdZero() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputA(jsc());
		JavaRDD<Tuple3<Long, Long, Double>> expected = similarities(new Tuple3<>(0l, 1l, 0.8943165213045865d));
		
		JavaRDD<Tuple3<Long, Long, Double>> actual = RowMatrixNearDuplicateDetection.similarColumns(input, 0d).toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}
	
	@Test
	public void testSelfSimililaritiesForAWithThreshold0_9() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputA(jsc());
		JavaRDD<Tuple3<Long, Long, Double>> expected = similarities();
		
		JavaRDD<Tuple3<Long, Long, Double>> actual = RowMatrixNearDuplicateDetection.similarColumns(input, 0.9d).toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}
	
	@Test
	public void testRowMatrixForB() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputB(jsc());
		
		JavaRDD<Vector> expected = DocumentWordVectorFileToRowMatrixTest.vectors(jsc(),
				new double[] { 1, 0, 1, 0.99 },
				new double[] { 0, 1, 1, 0.99 });
		
		JavaRDD<Vector> actual = RowMatrixNearDuplicateDetection.asRowTransposedMatrix(input.toJavaRDD()).rows().toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}
	
	private JavaRDD<Tuple3<Long, Long, Double>> similarities(Tuple3<Long, Long, Double>...entries) {
		return jsc().parallelize(Stream.of(entries).collect(Collectors.toList()));
	}
	
	@Test
	public void testSelfSimililaritiesForBWithThresholdZero() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputB(jsc());
		JavaRDD<Tuple3<Long, Long, Double>> expected = similarities(
			new Tuple3<>(2l, 3l, 0.9999999999999998d),
			new Tuple3<>(1l, 2l, 0.7071067811865475d),
			new Tuple3<>(0l, 3l, 0.7071067811865475d),
			new Tuple3<>(1l, 3l ,0.7071067811865475d),
			new Tuple3<>(0l ,2l ,0.7071067811865475d)
		);
		
		JavaRDD<Tuple3<Long, Long, Double>> actual = RowMatrixNearDuplicateDetection.similarColumns(input, 0d).toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}
	
	@Test
	public void testSelfSimililaritiesForBWithThreshold0_9() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputB(jsc());
		JavaRDD<Tuple3<Long, Long, Double>> expected = similarities(
			new Tuple3<>(2l, 3l, 0.9999999999999998d)
		);
		
		JavaRDD<Tuple3<Long, Long, Double>> actual = RowMatrixNearDuplicateDetection.similarColumns(input, 0.8d).toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}
	
	@Test
	public void testSelfSimililaritiesForCWithThresholdZero() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputC(jsc());
		JavaRDD<Tuple3<Long, Long, Double>> expected = similarities(
			new Tuple3<>(2l, 3l, 0.7999999999999999d),
			new Tuple3<>(0l, 1l, 0.19999999999999998d),
			new Tuple3<>(1l, 2l, 0.7999999999999999d),
			new Tuple3<>(0l, 3l, 0.7999999999999999d),
			new Tuple3<>(1l, 3l, 0.39999999999999997d),
			new Tuple3<>(0l, 2l, 0.39999999999999997d)
		);
		
		JavaRDD<Tuple3<Long, Long, Double>> actual = RowMatrixNearDuplicateDetection.similarColumns(input, 0d).toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}

	@Test
	public void testSelfSimililaritiesForCWithThreshold0_5() {
		RDD<String> input = DocumentWordVectorFileToRowMatrixTest.inputC(jsc());
		JavaRDD<Tuple3<Long, Long, Double>> expected = similarities(
			new Tuple3<>(2l, 3l, 0.7999999999999999d),
			new Tuple3<>(1l, 2l, 0.7999999999999999d),
			new Tuple3<>(0l, 3l, 0.7999999999999999d)
		);
		
		JavaRDD<Tuple3<Long, Long, Double>> actual = RowMatrixNearDuplicateDetection.similarColumns(input, 0.5d).toJavaRDD();
		
		JavaRDDComparisons.assertRDDEquals(expected, actual);
	}
}
