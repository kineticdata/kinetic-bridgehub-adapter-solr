/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kineticdata.bridgehub.adapter.solr;

import com.kineticdata.bridgehub.adapter.solr.SolrAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Test;
import static org.junit.Assert.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author austin.peters
 */
public class SolrAdapterTest {
    
    private final String apiUrl = "http://localhost:8984/solr";
    private final String structure = "sample_techproducts_configs";
    
    @Test
    public void test_escapedSearchUrl() throws Exception {
        
        StringBuilder expectedUrl = new StringBuilder();
        String actualUrl = null;
        String logLevel = "This is an error.";
        String date = "2021-01-01";
        String pageSize = "1000";
        String offset = "0";
        String query = "message:\"<%= parameter[\"log level\"] %>\" AND _timestamp:><%= parameter[\"date\"] %>";
        
        BridgeRequest request = new BridgeRequest();
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL",apiUrl);
        
        request.setStructure(structure);
        request.setQuery(query);
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("log level", logLevel);
        bridgeParameters.put("date", date);
        request.setParameters(bridgeParameters);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", pageSize);
        bridgeMetadata.put("offset", offset);
        request.setMetadata(bridgeMetadata);
        
        expectedUrl.append(apiUrl)
            .append("/")
            .append(structure)
            .append("/")
            .append("select?q=message%3A%22This%5C+is%5C+an%5C+error.%22+AND+_timestamp%3A%3E2021%5C-01%5C-01")
            .append("&rows=")
            .append(pageSize)
            .append("&start=")
            .append(offset)
            .append("&wt=json");

        try {
            actualUrl = adapter.buildUrl("search", request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedUrl.toString(), actualUrl);
        
        try {
            bridgeMetadata.put("order", "<%=field[\"_timestamp\"]%>:DESC,<%=field[\"_source.message\"]%>:ASC");
            request.setMetadata(bridgeMetadata);
            actualUrl = adapter.buildUrl("search", request);
        } catch (BridgeError e) {
            throw new RuntimeException(e);
        }
        
        expectedUrl.delete(expectedUrl.indexOf("&wt=json"), expectedUrl.indexOf("&wt=json") + 8);
        expectedUrl.append("&sort=_timestamp%3Adesc%2Cmessage%3Aasc&wt=json");
        assertEquals(expectedUrl.toString(), actualUrl);
        
    }
    
    @Test
    public void testCountResults() throws Exception {
        Integer expectedCount = 1;
        String expectedUrl = String.format("%s/%s/select?q=message%%3Aerror&rows=0&wt=json", apiUrl, structure);
        Count actualCount;
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL",apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("log level", "error");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);        
        request.setStructure(structure);
        request.setQuery("message:<%= parameter[\"log level\"] %>");
        
        assertEquals(expectedUrl, adapter.buildUrl("count", request));
        
        try {
            actualCount = adapter.count(request);
        } catch (BridgeError e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        
        assertEquals(expectedCount, actualCount.getValue());
    }
    
    @Test
    public void testRetrieveResults() throws Exception {
        String expectedUrl = String.format("%s/%s/select?q=message%%3Aerror&rows=1000&start=0&fl=message%%2C_timestamp&wt=json", apiUrl, structure);
        
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put("Username",null);
        configuration.put("Password",null);
        configuration.put("Solr URL", apiUrl);
        
        SolrAdapter adapter = new SolrAdapter();
        adapter.setProperties(configuration);
        adapter.initialize();
        
        Map<String, String> bridgeParameters = new HashMap<String, String>();
        bridgeParameters.put("log level", "error");
        
        Map<String, String> bridgeMetadata = new HashMap<String, String>();
        bridgeMetadata.put("pageSize", "1000");
        bridgeMetadata.put("offset", "0");        
        
        BridgeRequest request = new BridgeRequest();
        request.setParameters(bridgeParameters);
        request.setMetadata(bridgeMetadata);
        request.setStructure(structure);
        request.setQuery("message:<%= parameter[\"log level\"] %>");
        request.setFields(
            Arrays.asList(
                "message",
                "_timestamp"
            )
        );
        
        assertEquals(expectedUrl, adapter.buildUrl("search", request));
        
        Record bridgeRecord = adapter.retrieve(request);
        
    }    
    
    @Test
    public void testMappingResponseToFields() throws Exception {
        
        SolrAdapter adapter = new SolrAdapter();
        StringBuilder fieldPrefix = new StringBuilder();
        Map<String, Object> actualBridgeFields = new HashMap<String, Object>();
        Map<String, Object> expectedBridgeFields = new HashMap<String, Object>();
        
        expectedBridgeFields.put("_id", "AV2y_WlvnjHdd-LJ52Y1");
        expectedBridgeFields.put("_index", "examples");
        expectedBridgeFields.put("_score", "0.68064547");
        expectedBridgeFields.put("_type", "doc");
        
        expectedBridgeFields.put("_source.message", "this is an error message.");
        expectedBridgeFields.put("_source.app.username", "testuser");
        expectedBridgeFields.put("_source.app.name", "Bridgehub");
        expectedBridgeFields.put("_source.access.path", "NUNYA");
        expectedBridgeFields.put("_source.number test", "25");
        
        String apiResponse = "{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0},\"hits\":{\"total\":1,\"max_score\":0.68064547,\"hits\":[{\"_index\":\"examples\",\"_type\":\"doc\",\"_id\":\"AV2y_WlvnjHdd-LJ52Y1\",\"_score\":0.68064547,\"_source.message\":\"this is an error message.\",\"_source.app.username\":\"testuser\",\"_source.app.name\":\"Bridgehub\",\"_source.number test\":25,\"_source.access.path\":\"NUNYA\"}]}}";

        // Parse the Response String into a JSON Object
        JSONObject json = (JSONObject)JSONValue.parse(apiResponse);
        // Get an array of objects from the parsed json
        JSONObject hints = (JSONObject)json.get("hits");
        JSONArray hitsArray = (JSONArray)hints.get("hits");
        
        adapter.mapToFields((JSONObject)hitsArray.get(0), fieldPrefix, actualBridgeFields);
        
        assertThat(actualBridgeFields, is(expectedBridgeFields));
        
    }
    
}
