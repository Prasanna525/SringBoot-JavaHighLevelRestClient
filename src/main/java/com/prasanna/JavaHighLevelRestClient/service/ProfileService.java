package com.prasanna.JavaHighLevelRestClient.service;

import static com.prasanna.JavaHighLevelRestClient.util.Constant.INDEX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prasanna.JavaHighLevelRestClient.index.ProfileDocument;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class ProfileService {
	
	private RestHighLevelClient client;
	
	private ObjectMapper objectMapper;

	@Autowired
	public ProfileService(RestHighLevelClient client, ObjectMapper objectMapper) {
		
		this.client = client;
		this.objectMapper = objectMapper;
	}
	
	public String createProfile(ProfileDocument document) throws Exception{
		
		UUID uuid = UUID.randomUUID();
		document.setId(uuid.toString());
		
		@SuppressWarnings("unchecked")
		Map<String, Object> documentMapper = objectMapper.convertValue(document, Map.class);//profileToMap
		
		IndexRequest indexRequest = new IndexRequest(INDEX).source(documentMapper);
		
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		
		return indexResponse.getResult().name();
	}
	 
	public ProfileDocument findById(String id) throws Exception{
		
		GetRequest getRequest = new GetRequest(INDEX,id);
		
		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
		
		Map<String, Object> resultMap = getResponse.getSourceAsMap();
		
		return objectMapper.convertValue(resultMap, ProfileDocument.class);//MapToProfile
	}
	
	public List<ProfileDocument> findAll() throws Exception{
		
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(INDEX);
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		return getSearchResult(searchResponse);
	}
	
	private List<ProfileDocument> getSearchResult(SearchResponse response) {

        SearchHit[] searchHit = response.getHits().getHits();

        List<ProfileDocument> profileDocuments = new ArrayList<>();

        if (searchHit.length > 0) {

            Arrays.stream(searchHit)
                    .forEach(hit -> profileDocuments
                            .add(objectMapper
                                    .convertValue(hit.getSourceAsMap(),
                                                    ProfileDocument.class))
                    );
        }

        return profileDocuments;
    }
	
	public String updateProfile(ProfileDocument document) throws Exception{
		
		ProfileDocument resultDocument = findById(document.getId());
		
		UpdateRequest updateRequest = new UpdateRequest(INDEX, resultDocument.getId());
		
		Map<String, Object> documentMapper = objectMapper.convertValue(document, Map.class);
		
		updateRequest.doc(documentMapper);
		
		UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
		
		return updateResponse.getResult().name();
	}
	
	public List<ProfileDocument> searchByTechnology(String technology) throws Exception {

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder queryBuilder = QueryBuilders
                .boolQuery()
                .must(QueryBuilders
                        .matchQuery("technologies.name", technology));

        searchSourceBuilder.query(QueryBuilders
                .nestedQuery("technologies",
                        queryBuilder,
                        ScoreMode.Avg));

        searchRequest.source(searchSourceBuilder);

        SearchResponse response =
                client.search(searchRequest, RequestOptions.DEFAULT);

        return getSearchResult(response);
    }
	
	public String deleteProfileDocument(String id) throws Exception {

        DeleteRequest deleteRequest = new DeleteRequest(INDEX, id);
        DeleteResponse response =
                client.delete(deleteRequest, RequestOptions.DEFAULT);

        return response
                .getResult()
                .name();

    }
	
}
