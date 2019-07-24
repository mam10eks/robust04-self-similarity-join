package de.webis.trec_ndd.trec_collections;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.anserini.eval.QueryJudgments;

public class QrelReader {
	public Set<String> readJudgedDocumentIdsFromQrelsFile(String qrelsFile) throws IOException {
		return new QueryJudgments(qrelsFile)
				.getQrels().values().stream()
				.map(Map::keySet)
				.flatMap(Set::stream)
				.collect(Collectors.toSet());
	}
}
