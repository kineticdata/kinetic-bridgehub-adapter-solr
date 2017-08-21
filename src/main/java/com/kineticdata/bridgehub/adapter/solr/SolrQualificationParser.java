package com.kineticdata.bridgehub.adapter.solr;

import com.kineticdata.bridgehub.adapter.QualificationParser;
import org.apache.commons.lang.StringUtils;

public class SolrQualificationParser extends QualificationParser {
    @Override
    public String encodeParameter(String name, String value) {
        String result = null;
        //http://lucene.apache.org/core/4_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Escaping_Special_Characters
        //Next three lines: escape the following characters with a backslash: + - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /  
        String regexReservedCharactersPattern = "(\\*|\\+|\\-|\\=|\\~|\\>|\\<|\\\"|\\?|\\^|\\$|\\{|\\}|\\(|\\)|\\:|\\!|\\/|\\[|\\]|\\\\|\\s)";
        if (StringUtils.isNotEmpty(value)) {
            result = value.replaceAll(regexReservedCharactersPattern, "\\\\$1")
                .replaceAll("\\|\\|", "\\||")
                .replaceAll("\\&\\&", "\\&&")
                .replaceAll("AND", "\\A\\N\\D")
                .replaceAll("OR", "\\O\\R")
                .replaceAll("NOT", "\\N\\O\\T");
        };
        return result;
    }
}
