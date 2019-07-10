package de.webis.trec_ndd;

import java.util.Arrays;

import org.apache.solr.update.processor.TextProfileSignature;
import org.apache.solr.client.solrj.SolrQuery;

public class TextProfileSignatureSimilarity implements TextSimilarity {

	@Override
	public boolean textsAreSimilar(String a, String b) {
		return Arrays.equals(textProfileSignature(a), textProfileSignature(b));
	}

	private static byte[] textProfileSignature (String text) {
		TextProfileSignature signature = new TextProfileSignature();
		signature.init(new SolrQuery());
		signature.add(text);
		return signature.getSignature();
	}
}
