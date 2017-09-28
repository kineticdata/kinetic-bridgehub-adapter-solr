package com.kineticdata.bridgehub.adapter.solr;

import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.QualificationParser;
import static com.kineticdata.bridgehub.adapter.QualificationParser.PARAMETER_PATTERN;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

public class SolrQualificationParser extends QualificationParser {
    
    //public static String PARAMETER_PATTERN = "<%=\\s*parameter\\[\\\"?(.*?)\\\"?\\]\\s*%>";
    public static String PARAMETER_PATTERN_JSON_SAFE = "<%= parameter\\['(.*?)'\\] %>";
    public static String PARAMETER_PATTERN_GROUP_MATCH = "<%=\\s*parameter\\[\\s*\"(.*?)\"\\s*\\]\\s*%>";
    public static String QUERY_PATTERN_JSON = "^\\s*\\{.*?\\}\\s*$";
    public static String QUERY_PATTERN_KINETIC = "^\\s*\\\\[\\s*\\{.*?\\}\\s*,\\s*" + PARAMETER_PATTERN + "\\s*\\\\]\\s*$";
    public static String QUERY_PATTERN_SOLR_JSON = "^\\s*\\\\[\\s*\\{.*?\\}\\s*,\\s*\\\\{.*?\\\\}\\s*\\\\]\\s*$";
    
    public static String QUERY_STYLE_KINETIC = "Kinetic DSL";
    public static String QUERY_STYLE_SOLR = "Solr DSL";
    public static String QUERY_STYLE_LUCENE = "lucene";
    
    public static String METADATA_FIELD_CONCATE_OPERATOR = "concatenatingOperator";
    public static String METADATA_FIELD_QUERY_PREFIX = "queryPrefix";
    public static String METADATA_FIELD_QUERY_STYLE = "type";
    public static String METADATA_FIELD_WHITELISTED_FIELDS = "whitelistedFields";
    
    @Override
    public String encodeParameter(String name, String value) {
        String result = null;
        //http://lucene.apache.org/core/4_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Escaping_Special_Characters
        //Next three lines: escape the following characters with a backslash: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /  
        String regexReservedCharactersPattern = "(\\*|\\+|\\-|\\=|\\~|\\>|\\<|\\\"|\\?|\\^|\\$|\\{|\\}|\\(|\\)|\\:|\\!|\\/|\\[|\\]|\\\\|\\s)";
        if (StringUtils.isNotEmpty(value)) {
            result = value.replaceAll(regexReservedCharactersPattern, "\\\\$1")
                .replaceAll("\\|\\|", "\\\\||")
                .replaceAll("\\&\\&", "\\\\&&")
                .replaceAll("AND", "\\\\A\\\\N\\\\D")
                .replaceAll("OR", "\\\\O\\\\R")
                .replaceAll("NOT", "\\\\N\\\\O\\\\T");
        }
        return result;
    }
    
    @Override
    public String parse(String query, Map<String, String> parameters) throws BridgeError {

        Map<String, Object> queryMetadata = null;
        String parsedQuery = new String();
        
        // Change <%= parameter["asdf"] %> to <%= parameter['asdf'] %> so we can safely-ish parse the bridge query JSON.
        query = query.replaceAll(PARAMETER_PATTERN_GROUP_MATCH, "<%= parameter['$1'] %>");
        boolean metadataDetected = query.matches(QUERY_PATTERN_JSON);
        if (metadataDetected) {
            try {
                queryMetadata = (Map<String, Object>)JSONValue.parseWithException(query);
            } catch (ParseException exceptionDetails) {
                throw new BridgeError(
                    String.format("The bridge query (%s) appears to be a JSON Object " +
                    "instead of a lucene query because it starts and ends with curly braces." +
                    " The query failed however to parse successfully as JSON.", query),
                    exceptionDetails
                );
            }
            String queryType = (String)queryMetadata.get("type");
            String jsonQuery = (String)queryMetadata.get("query");

            if (StringUtils.equalsIgnoreCase(queryType, QUERY_STYLE_KINETIC)) {
                String concateOperator = (String)queryMetadata.get("concateOperator");
                String queryPrefix = (String)queryMetadata.get("queryPrefix");
                List<String> whitelistFields = (List<String>)queryMetadata.get("whitelistFields");
                parsedQuery = parseKineticDsl(
                    queryPrefix, 
                    whitelistFields, 
                    concateOperator, 
                    jsonQuery, 
                    parameters
                );
            } else if (StringUtils.equalsIgnoreCase(queryType, QUERY_STYLE_SOLR)) {
                parsedQuery = parseSolrDsl(true, jsonQuery, parameters);
            } else {
                throw new BridgeError(
                    String.format(
                        "The specified query type \"%s\" is not valid. Valid options are: %s",
                            queryType,
                            Arrays.asList(QUERY_STYLE_KINETIC, QUERY_STYLE_SOLR)
                    )
                );
            }
            
        } else {
            parsedQuery = parseSolrDsl(false, query, parameters);
        }
        
        return parsedQuery;
        
    }
    
    private String parseKineticDsl(String queryPrefix, List<String> whitelistedFields, String concateOperator, String jsonQuery, Map<String, String> parameters) throws BridgeError {

        Map<String, Object> queryConcatenation = new HashMap();
        StringBuilder query = new StringBuilder();
        if (queryPrefix != null) {
            query.append(queryPrefix);
        }
        if (StringUtils.isBlank(concateOperator)) {
            concateOperator = "AND";
        }
        
        try {
            queryConcatenation = (Map<String, Object>)JSONValue.parseWithException(jsonQuery);
        } catch (ParseException exceptionDetails) {
            throw new BridgeError(
                String.format("The JSON query parameter value (%s) did not parse successfully as JSON.", jsonQuery),
                exceptionDetails
            );
        }
        
        boolean firstRun = true;
        for (Map.Entry<String, Object> queryPartial : queryConcatenation.entrySet()) {
            
            if (firstRun == false) {
                query
                    .append(" ")
                    .append(concateOperator.trim())
                    .append(" ");
            }
            
            String fieldName = queryPartial.getKey();
            if (whitelistedFields == null || whitelistedFields.contains(fieldName)) {
                Map<String, Object> fieldProperties = (Map<String, Object>)queryPartial.getValue();
                String matchType = (String)fieldProperties.get("matcher");
                if (matchType == null) {
                    matchType = "exact";
                }
                String fieldValue = (String)fieldProperties.get("value");
                if (fieldValue != null) {
                    query.append(fieldName)
                        .append(":");
                    if (matchType.equals("endsWith") || matchType.equals("like")) {
                        query.append("*");
                    }
                    query.append(encodeParameter(fieldName, fieldValue));
                    if (matchType.equals("startsWith") || matchType.equals("like")) {
                        query.append("*");
                    }
                } else {
                    throw new BridgeError(
                        String.format(
                            "The %s field is missing a value key in the Kinetic Solr DSL JSON: %s",
                            fieldName,
                            jsonQuery
                        )
                    );
                }
                
            }
            
            firstRun = false;
            
        }
        
        if (StringUtils.isEmpty(query.toString())) {
            throw new BridgeError (
                String.format(
                    "Unable to produce a lucene query from the following Kinetic DSL structure: %s",
                    jsonQuery
                )
            );
        }
        
        return query.toString();
    }

    private String parseSolrDsl(boolean isJsonQuery, String solrQuery, Map<String, String> parameters) throws BridgeError {
        
        StringBuffer resultBuffer = new StringBuffer();
        Pattern pattern = Pattern.compile(PARAMETER_PATTERN_JSON_SAFE);
        Matcher matcher = pattern.matcher(solrQuery);

        while (matcher.find()) {
            // Retrieve the necessary values
            String parameterName = matcher.group(1);
            // If there were no parameters provided
            if (parameters == null) {
                throw new BridgeError("Unable to parse qualification, "+
                    "the '"+parameterName+"' parameter was referenced but no "+
                    "parameters were provided.");
            }
            String parameterValue = parameters.get(parameterName);
            // If there is a reference to a parameter that was not passed
            if (parameterValue == null) {
                throw new BridgeError("Unable to parse qualification, "+
                    "the '"+parameterName+"' parameter was referenced but "+
                    "not provided.");
            }

            String value;
            // If the query string starts with a curly brace, this is a JSON payload.
            // else it is supposed to be a query used for the q parameter in a URI Search
            if (isJsonQuery) {
                // if JSON, escape any JSON special characters.
                value = JSONValue.escape(encodeParameter(parameterName, parameterValue));
            } else {
                // if not JSON, encode the parameter by escaping any Lucene query syntax reserved characters.
                value = encodeParameter(parameterName, parameterValue);
            }
            matcher.appendReplacement(resultBuffer, Matcher.quoteReplacement(value));
        }

        matcher.appendTail(resultBuffer);
        return resultBuffer.toString();

    }
    
}
