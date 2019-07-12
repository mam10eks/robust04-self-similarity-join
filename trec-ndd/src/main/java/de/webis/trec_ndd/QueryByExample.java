package de.webis.trec_ndd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class QueryByExample {	
	public static String esQueryByExample(String document){
		return esOrQuery(tokensInText(document));
	}
	
	public static String esQueryByExampleWithKMedianTokens(String document, int k) {
		List<String> sortedTokens = new ArrayList<String>(tokensInText(document));
		Collections.sort(sortedTokens, (a,b) -> WordCounts.getWordCount(a).compareTo(WordCounts.getWordCount(b)));
		Set<String> ret = new HashSet<>();
		
		while(k > ret.size() && sortedTokens.size() > 0) {
			ret.add(sortedTokens.remove(sortedTokens.size()/2));
		}
		
		return esOrQuery(ret);
	}
	
	private static String esOrQuery(Collection<String> tokens) {
		return tokens.stream()
			.map(t -> "(body_lang.en: \"" + t + "\")")
			.collect(Collectors.joining(" OR "));
	}

	private static Set<String> tokensInText(String text) {
		try (Analyzer analyzer = new StandardAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET)) {
			return tokensInText(analyzer, text);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static Set<String> tokensInText(Analyzer analyzer, String text) throws IOException {
		Set<String> ret = new HashSet<>();
		TokenStream tokenStream = analyzer.tokenStream("", text);

		CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();

		while (tokenStream.incrementToken()) {
			ret.add(attr.toString());
		}

		return ret;
	}
}
