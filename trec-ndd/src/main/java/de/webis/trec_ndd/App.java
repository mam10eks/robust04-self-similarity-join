package de.webis.trec_ndd;

import java.io.IOException;
import java.util.Set;

import org.apache.http.HttpHost;

import de.webis.trec_ndd.es.ElasticSearchClient;
import de.webis.trec_ndd.similarity.TextProfileSignatureSimilarity;
import de.webis.trec_ndd.similarity.TextSimilarity;
import de.webis.trec_ndd.trec_collections.QrelReader;
import de.webis.trec_ndd.util.QueryByExampleBuilder;

public class App {
	public static void main(String[] args) throws IOException {
		System.out.println("Please note that you must be connected to the vpn ;)");
		
		executeExampleReadOnlyRequestsProductionElasticsearch();
		readSomeQrelFiles();
		doSomeNearDuplicateDetection();
	}
	
	private static void executeExampleReadOnlyRequestsProductionElasticsearch() throws IOException {
		//Attention: Our production system. Please use only read operations!
		ElasticSearchClient client = productionCluewebElasticSearch();
		
		String documentId = "340b9756-040d-5a2d-b062-d351fb7ca660";
		String documentText = client.getTextOfDocument(documentId);
		System.out.println("The full text of document '"+ 
				documentId +"' is: " + documentText);
		
		String query = QueryByExampleBuilder.esQueryByExampleWithKMedianTokens(documentText, 5);
		System.out.println("I use the query '"+ query +"' to obtain near duplicates.");
		
		System.out.println(client.getDocumentsForQuery(query, 3));
	}
	
	private static void readSomeQrelFiles() throws IOException {
		String exampleQrelFile = "topics-and-qrels/qrels.web.51-100.txt";
		Set<String> judgedDocumentIds = new QrelReader().readJudgedDocumentIdsFromQrelsFile(exampleQrelFile);
		
		System.out.println("The qrel-file '"+ exampleQrelFile 
				+"' has the following documents with judgements: "
				+ judgedDocumentIds);
	}

	private static void doSomeNearDuplicateDetection() {
		TextSimilarity textSimilarity = new TextProfileSignatureSimilarity();
		String textOfDocumentA = "Hello World";
		String textOfDocumentB = "Hello World";
		
		System.out.println("Dummy documents are near-duplicates: "
				+ textSimilarity.textsAreSimilar(textOfDocumentA, textOfDocumentB)
		);
	}
	
	private static ElasticSearchClient productionCluewebElasticSearch() {
		String index = "webis_warc_clueweb12_011";
		HttpHost productionHost = new HttpHost("betaweb120", 9200, "http");
		
		return new ElasticSearchClient(productionHost, index);
	}
}
