package io.hdocdb.execute;

import io.hdocdb.HDocument;
import org.ojai.Document;

import java.util.Map;

public class QueryInfo {

    public enum QueryType {
        FULL_TABLE_SCAN,
        INDEX_SCAN
    }

    private QueryType type;
    private String indexName;
    private Map<String, String> indexBounds;
    private int scannedIndexesCount;
    private int staleIndexesRunningCount;

    public QueryInfo(QueryType type) {
        this.type = type;
    }

    public QueryInfo(QueryType type, String indexName, Map<String, String> indexBounds,
                     int scannedIndexesCount, int staleIndexesRunningCount) {
        this.type = type;
        this.indexName = indexName;
        this.indexBounds = indexBounds;
        this.scannedIndexesCount = scannedIndexesCount;
        this.staleIndexesRunningCount = staleIndexesRunningCount;
    }

    public QueryType getType() {
        return type;
    }

    public String getIndexName() {
        return indexName;
    }

    public Map<String, String> getIndexBounds() {
        return indexBounds;
    }

    public int getScannedIndexesCount() {
        return scannedIndexesCount;
    }

    public int getStaleIndexesRunningCount() {
        return staleIndexesRunningCount;
    }

    public Document asDocument() {
        Document doc = new HDocument();
        if (type == QueryType.FULL_TABLE_SCAN) {
            doc.set("plan", "full table scan");
        } else {
            doc.set("plan", "index scan");
            doc.set("indexName", indexName);
            doc.set("indexBounds", indexBounds);
            doc.set("staleIndexesRunningCount", staleIndexesRunningCount);
        }
        return doc;
    }

}

