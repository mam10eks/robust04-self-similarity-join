package de.webis.trec_ndd.trec_collections;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import io.anserini.collection.SourceDocument;

public interface CollectionReader<T extends SourceDocument> {
	public List<Document> extractJudgedRawDocumentsFromCollection();

	public List<Document> extractRawDocumentsFromCollection();
	
	public default List<CollectionDocument> extractJudgedDocumentsFromCollection() { 
		return extractJudgedRawDocumentsFromCollection().stream()
				.map(CollectionDocument::fromLuceneDocument)
				.collect(Collectors.toList());
	}
}
