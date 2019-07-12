package de.webis.trec_ndd.trec_collections;

import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Test;

import io.anserini.collection.SourceDocument;

public class AnseriniCollectionReaderTest<T extends SourceDocument> {
	@Test
	public void approveTransformationOfSmallCollection() {
		CollectionReader<T> reader = robustAnseriniCollectionReader();
		List<CollectionDocument> documents = reader.extractJudgedDocumentsFromCollection();

		Approvals.verifyAsJson(documents);
	}
	
	@Test
	public void approveTransformationOfSmallCluewebSampleCollection() {
		CollectionReader<T> reader = cluewebAnseriniCollectionReader();
		List<CollectionDocument> documents = reader.extractJudgedDocumentsFromCollection();

		Approvals.verifyAsJson(documents);
	}

	private static <T extends SourceDocument> CollectionReader<T> robustAnseriniCollectionReader() {
		String pathToCollection = "../collection_to_doc_vectors/test/data/robust";
		String pathToQrels = "../collection_to_doc_vectors/test/data/robust-qrels.txt";
		String collectionType = "TrecCollection";
		String documentGenerator = "JsoupGenerator";

		return new AnseriniCollectionReader<T>(pathToCollection, pathToQrels, collectionType, documentGenerator);
	}
	
	private static <T extends SourceDocument> CollectionReader<T> cluewebAnseriniCollectionReader() {
		String pathToCollection = "../collection_to_doc_vectors/test/data/clueweb09-sample";
		String pathToQrels = "../collection_to_doc_vectors/test/data/clueweb09-qrels.txt";
		String collectionType = "ClueWeb09Collection";
		String documentGenerator = "JsoupGenerator";

		return new AnseriniCollectionReader<T>(pathToCollection, pathToQrels, collectionType, documentGenerator);
	}
}
