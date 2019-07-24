package de.webis.trec_ndd.trec_collections;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;

import io.anserini.collection.DocumentCollection;
import io.anserini.collection.Segment;
import io.anserini.collection.SegmentProvider;
import io.anserini.collection.SourceDocument;
import io.anserini.eval.QueryJudgments;
import io.anserini.index.IndexCollection;
import io.anserini.index.IndexCollection.Args;
import io.anserini.index.IndexCollection.Counters;
import io.anserini.index.generator.LuceneDocumentGenerator;
import lombok.Data;
import lombok.SneakyThrows;

@Data
public class AnseriniCollectionReader<T extends SourceDocument> implements CollectionReader<T> {
	private final String pathToCollection,
						pathToQrels,
						collectionType,
						documentGenerator;

	@Override
	@SneakyThrows
	public List<Document> extractJudgedRawDocumentsFromCollection() {
		Set<String> judgedDocumentIds = judgedDocumentIds();
		
		return extractRawDocumentsFromCollection().stream()
				.filter(doc -> judgedDocumentIds.contains(documentId(doc)))
				.collect(Collectors.toList());
	}

	@Override
	@SneakyThrows
	public List<Document> extractRawDocumentsFromCollection() {
		List<Document> ret = new LinkedList<>();
		LuceneDocumentGenerator<T> generator = documentGenerator();
		SegmentProvider<T> collection = documentCollection();

		for (Path segmentPath : collection.getFileSegmentPaths()) {
			Segment<T> segment = collection.createFileSegment(segmentPath);
			
			while(segment.hasNext()) {
				T document = segment.next();
			
				if (!document.indexable()) {
					continue;
				}

				Document doc = generator.createDocument(document);
				if (doc != null) {
					ret.add(doc);
				}
			}
		}

		return ret;
	}

	@SneakyThrows
	private Set<String> judgedDocumentIds() {
		return new QrelReader().readJudgedDocumentIdsFromQrelsFile(pathToQrels);
	}
	
	@SneakyThrows
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private SegmentProvider<T> documentCollection() {
		SegmentProvider<T> ret = (SegmentProvider) Class.forName("io.anserini.collection." + args().collectionClass).newInstance();
		((DocumentCollection)ret).setCollectionPath(Paths.get(pathToCollection));
		
		return ret;
	}

	@SneakyThrows
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private LuceneDocumentGenerator<T> documentGenerator() {
		Args args = args();
		Class<?> clazz = Class.forName("io.anserini.index.generator." + args.generatorClass);

		return (LuceneDocumentGenerator) clazz.getDeclaredConstructor(Args.class, Counters.class).newInstance(args,
				new IndexCollection(args).new Counters());
	}
	


	private Args args() {
		Args ret = new Args();
		ret.generatorClass = documentGenerator;
		ret.collectionClass = collectionType;
		ret.index = "-index";
		ret.input = pathToCollection;
		
		return ret;
	}
	
	static String documentId(Document doc) {
		return doc.getField(LuceneDocumentGenerator.FIELD_ID).stringValue();
	}
}
