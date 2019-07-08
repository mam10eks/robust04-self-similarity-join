package de.webis.trec_ndd;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public class DocumentWordVectorFileToRowMatrixTest extends SharedJavaSparkContext {
	@Test
	public void test() {
		JavaRDD<String> input = input("{\"content\": [0.020, 0.1, -0.3], \"docid\": \"a\"}",
				"{\"content\": [0.010, 0.2, -0.2], \"docid\": \"b\"}",
				"{\"content\": [-0.010, -0.1, 0.2], \"docid\": \"c\"}");
		RowMatrix actualMatrix = SparkNearDuplicateDetection.documentVectorToRowMatrix(input);
		JavaRDD<Vector> actual = actualMatrix.rows().toJavaRDD();

		JavaRDD<Vector> expected = vectors(new double[] { 0.020, 0.1, -0.3 }, new double[] { 0.010, 0.2, -0.2 },
				new double[] { -0.010, -0.1, 0.2 });
//		MatrixEntry
//		actual.rows()
//		actual.columnSimilarities().entries().first().
//		IndexedRowMatrix a = new IndexedRow(index, vector)

//		actual.
		assertRDDEquals(expected, actual);
	}

	@Test
	public void testThatAssertRddEqualsIsWorking() {
		JavaRDD<String> input = input("{\"content\": [0.1], \"docid\": \"a\"}");
		JavaRDD<Vector> actual = SparkNearDuplicateDetection.documentVectorToRowMatrix(input).rows().toJavaRDD();
		JavaRDD<Vector> expected = vectors(new double[] { 0.2 });

		try {
			assertRDDEquals(expected, actual);
		} catch (AssertionError e) {
			return;
		}

		Assert.fail("assertRDDEquals should fail");
	}

	private void assertRDDEquals(JavaRDD<Vector> a, JavaRDD<Vector> b) {
		JavaRDDComparisons.assertRDDEquals(a.map(Object::toString), b.map(Object::toString));
	}

	private JavaRDD<String> input(String... lines) {
		return jsc().parallelize(Arrays.asList(lines));
	}

	private JavaRDD<Vector> vectors(double[]... vectors) {
		return jsc().parallelize(Stream.of(vectors).map(vector -> Vectors.dense(vector)).collect(Collectors.toList()));
	}
}
