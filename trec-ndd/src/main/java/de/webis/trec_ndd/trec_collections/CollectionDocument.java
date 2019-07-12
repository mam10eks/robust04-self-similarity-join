package de.webis.trec_ndd.trec_collections;

import org.apache.lucene.document.Document;

import io.anserini.index.generator.LuceneDocumentGenerator;
import lombok.Data;

@Data
public class CollectionDocument {
	private final String id,
						content;
	
	public static CollectionDocument fromLuceneDocument(Document document) {
		return new CollectionDocument(
				AnseriniCollectionReader.documentId(document),
				document.get(LuceneDocumentGenerator.FIELD_BODY)
		);
	}
}
