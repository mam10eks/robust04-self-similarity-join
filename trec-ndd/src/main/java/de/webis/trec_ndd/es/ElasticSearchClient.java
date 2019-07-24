package de.webis.trec_ndd.es;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;

import com.jayway.jsonpath.JsonPath;

public class ElasticSearchClient {
	
	public static final String DOCUMENT_TYPE = "warcrecord";
	
	private final HttpHost host;
	
	private final String index;
	
	public ElasticSearchClient(HttpHost host, String index) {
		this.host = host; 
		this.index = index;
	}



	
	public List<Map<String, Object>> getDocumentsForQuery(String query, int size) throws IOException {
		Request request = new Request("GET", index + "/_search");
		request.addParameter("q", query);
		request.addParameter("size", String.valueOf(size));
		
		String response = executeRequestAndReturnResponseBody(request);		

		return JsonPath.read(response, "$.['hits'].['hits']");
	}
	
	public String getTextOfDocument(String documentId) throws IOException {
		Request request = new Request("GET", index + "/" + DOCUMENT_TYPE + "/" + documentId);
		String response = executeRequestAndReturnResponseBody(request);		

		return JsonPath.read(response, "$.['_source'].['body_lang.en']");
	}
	
	private String executeRequestAndReturnResponseBody(Request request) throws IOException {
		try (RestClient restClient = restClientBuilder(host).build()) {
			Response response = restClient.performRequest(request);
			return IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);	
		}
	}
	
	private static RestClientBuilder restClientBuilder(HttpHost host) {
		return  RestClient.builder(host).setRequestConfigCallback(new RequestConfigCallback() {
			@Override
			public Builder customizeRequestConfig(Builder requestConfigBuilder) {
				return requestConfigBuilder
	                    .setConnectTimeout(5000)
	                    .setSocketTimeout(60000);
			}
		});
	}
}
