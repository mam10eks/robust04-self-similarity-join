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
import org.junit.Test;

import com.jayway.jsonpath.JsonPath;

public class ElasticSearchTest {
	@Test
	public void checkEsClientCanBeCreated() throws IOException {
		RestClient client = productionElasticsearchClient();
		
		System.out.println(getDocumentsForQuery(client, "hallo welt", "webis_warc_clueweb12_011", 3));
	}
	
	public List<Map<String, Object>> getDocumentsForQuery(RestClient restClient, String query, String index, int size) throws IOException {
		Request request = new Request("GET", index + "/_search");
		request.addParameter("q", query);
		request.addParameter("size", String.valueOf(size));
		
		String response = executeRequestAndReturnResponseBody(restClient, request);		

		return JsonPath.read(response, "$.['hits'].['hits']");
	}
	
	public String executeRequestAndReturnResponseBody(RestClient restClient, Request request) throws IOException {
		Response response = restClient.performRequest(request);
		return IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
	}
	
	private static RestClient productionElasticsearchClient() {
		return restClientBuilder(new HttpHost("betaweb120", 9200, "http")).build();
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
