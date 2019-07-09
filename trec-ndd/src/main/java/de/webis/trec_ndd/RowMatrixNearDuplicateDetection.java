package de.webis.trec_ndd;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;

import scala.Tuple3;

public class RowMatrixNearDuplicateDetection {
	public static RowMatrix asRowTransposedMatrix(JavaRDD<String> input) {
		JavaRDD<IndexedRow> rows = input.zipWithIndex()
			.map(pair -> new IndexedRow(pair._2, SparkNearDuplicateDetection.documentLineToVector(pair._1)));
		
		BlockMatrix matrix = new IndexedRowMatrix(rows.rdd()).toBlockMatrix();
		matrix = matrix.cache();
		matrix.validate();
		matrix = matrix.transpose();
		
		return matrix.toIndexedRowMatrix().toRowMatrix();
	}
	
	public static RDD<Tuple3<Long, Long, Double>> similarColumns(RowMatrix matrix, double threshold) {
		return matrix.columnSimilarities(threshold)
			.entries().toJavaRDD()
			.filter(e -> e.value() >= threshold)
			.map(e -> new Tuple3<>(e.i(), e.j(), e.value())).rdd();
	}

	public static RDD<Tuple3<Long, Long, Double>> similarColumns(RDD<String> input, double threshold) {
		return similarColumns(asRowTransposedMatrix(input.toJavaRDD()), threshold);
	}
}
