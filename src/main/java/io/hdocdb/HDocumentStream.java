package io.hdocdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import io.hdocdb.execute.QueryInfo;
import io.hdocdb.store.HQueryCondition;
import io.hdocdb.store.IndexQueries;
import io.hdocdb.store.IndexQuery;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.DocumentListener;
import org.ojai.DocumentReader;
import org.ojai.DocumentStream;
import org.ojai.Value;
import org.ojai.exceptions.OjaiException;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HDocumentStream extends AbstractList<Document> implements DocumentStream {

    private static final Logger LOG = LoggerFactory.getLogger(HDocumentStream.class);

    private static final int DEFAULT_STALE_INDEX_EXPIRY_MS = 5000;
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    private List<HDocument> documents;
    private ResultScanner scanner;
    private Result[] results;
    private Table table;
    private IndexQueries indexQueries;
    private boolean reindexArrays;
    private QueryCondition condition;
    private String[] paths;
    private int index = 0;
    private int staleIndexesRunningCount = 0;

    public HDocumentStream(List<HDocument> documents, boolean reindexArrays, QueryCondition c, String... paths) {
        this.documents = documents;
        this.reindexArrays = reindexArrays;
        this.condition = c;
        this.paths = paths;
    }

    public HDocumentStream(ResultScanner scanner, boolean reindexArrays, QueryCondition c, String... paths) {
        this.scanner = scanner;
        this.reindexArrays = reindexArrays;
        this.condition = c;
        this.paths = paths;
    }

    public HDocumentStream(Result[] results, boolean reindexArrays, QueryCondition c, String... paths) {
        this.results = results;
        this.reindexArrays = reindexArrays;
        this.condition = c;
        this.paths = paths;
    }

    public HDocumentStream(Table table, IndexQueries indexQueries,
                           boolean reindexArrays, QueryCondition c, String... paths) {
        this.table = table;
        this.indexQueries = indexQueries;
        this.reindexArrays = reindexArrays;
        this.condition = c;
        this.paths = paths;
    }

    public void streamTo(DocumentListener l) {
        try {
            Iterator<Document> iter = iterator();

            while (iter.hasNext()) {
                Document doc = iter.next();
                l.documentArrived(doc);
            }
        } catch (Exception e) {
            try {
                close();
            } catch (Exception e2) {
                // noop
            }
            l.failed(e);
        }
    }

    public Iterator<Document> iterator() {
        return new Iterator<Document>() {
            HDocument next = null;
            boolean done = false;

            public boolean hasNext() {
                if (done) return false;
                if (next != null) return true;
                try {
                    next = HDocumentStream.this.next();
                    if (next == null) done = true;
                    return next != null;
                } catch (IOException e) {
                    throw new StoreException(e);
                }
            }

            public HDocument next() {
                if (!hasNext()) throw new NoSuchElementException();
                HDocument temp = next;
                next = null;
                return temp;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Iterable<DocumentReader> documentReaders() {
        return new DocumentReaderIterable();
    }

    public void close() throws OjaiException {
        if (scanner != null) scanner.close();
    }

    private HDocument next() throws IOException {
        HDocument doc = rawNext();
        while (doc != null && doc.isEmpty()) {
            doc = rawNext();
        }
        return doc;
    }

    private HDocument rawNext() throws IOException {
        if (documents != null) {
            HDocument doc = index < documents.size() ? documents.get(index++) : null;
            return doc;
        } else {
            Result result = null;
            IndexQuery indexQuery = null;
            if (scanner != null) {
                result = scanner.next();
            } else if (results != null) {
                result = index < results.length ? results[index++] : null;
            } else if (indexQueries != null) {
                if (index < indexQueries.size()) {
                    indexQuery = indexQueries.get(index++);
                    result = table.get(indexQuery.getQuery());
                }
            }
            HDocument doc = result != null ? new HDocument(result) : null;
            if (indexQuery != null) doc = checkIndexedDocument(doc, indexQuery);
            return doc != null && reindexArrays ? doc.reindexArrays() : doc;
        }
    }

    private HDocument checkIndexedDocument(HDocument doc, IndexQuery indexQuery) throws IOException {
        if (doc == null || doc.isEmpty()) {
            // only delete after some expiry as there is a timing issue between
            // index creation and document creation
            if (indexQuery.getIndexTs() + DEFAULT_STALE_INDEX_EXPIRY_MS < System.currentTimeMillis()) {
                deleteStaleIndex(indexQuery);
            }
        } else if (!indexQueries.getConditionFromRanges().evaluate(doc)) {
            deleteStaleIndex(indexQuery);
            doc = new HDocument();
        } else if (condition != null && !((HQueryCondition) condition).evaluate(doc)) {
            doc = new HDocument();
        } else {
            doc = project(doc);
        }
        return doc;

    }

    private void deleteStaleIndex(final IndexQuery indexQuery) throws IOException {
        staleIndexesRunningCount++;
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Delete delete = new Delete(Bytes.toBytes(indexQuery.getIndexRowKey()));
                    indexQueries.getIndexTable().delete(delete);
                } catch (IOException e) {
                    LOG.error("Could not delete stale index", e);
                }
            }
        });
    }

    private HDocument project(HDocument doc) {
        if (paths == null) return doc;
        HDocument newDoc = doc.shallowCopy();
        newDoc.clear();
        newDoc.setId(doc.getId());
        for (String path : paths) {
            Value value = doc.getValue(path);
            if (value != null && value.getType() != Value.Type.NULL) {
                newDoc.set(path, doc.getValue(path));
            }
        }
        return newDoc;
    }

    public QueryInfo explain() {
        return indexQueries != null
                ? new QueryInfo(QueryInfo.QueryType.INDEX_SCAN, indexQueries.getIndex().getName(),
                    getIndexBounds(), indexQueries.size(), staleIndexesRunningCount)
                : new QueryInfo(QueryInfo.QueryType.FULL_TABLE_SCAN);
    }

    private Map<String, String> getIndexBounds() {
        Map<String, String> bounds = Maps.newLinkedHashMap();
        for (int i = 0; i < indexQueries.getRanges().size(); i++) {
            Range<HValue> range = indexQueries.getRanges().get(i).getRange();
            bounds.put(indexQueries.getIndex().getField(i).getPath().asPathString(), range.toString());
        }
        return bounds;
    }

    final class DocumentReaderIterable implements Iterable<DocumentReader> {
        Iterator<Document> iterator = HDocumentStream.this.iterator();

        public Iterator<DocumentReader> iterator() {
            return new Iterator<DocumentReader>() {
                Document doc = null;

                public boolean hasNext() {
                    if (this.doc == null) {
                        if (iterator.hasNext()) {
                            this.doc = iterator.next();
                            return true;
                        } else {
                            return false;
                        }
                    } else {
                        return true;
                    }
                }

                public DocumentReader next() {
                    if (this.doc == null && !this.hasNext()) {
                        throw new NoSuchElementException();
                    } else {
                        Document old = this.doc;
                        this.doc = null;
                        return old.asReader();
                    }
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    /*
     * The following methods are for Nashorn integration
     */

    public List<HDocument> toList() {
        if (documents == null) {
            List<HDocument> list = Lists.newArrayList();
            for (Document doc : this) {
                list.add((HDocument) doc);
            }
            documents = list;
        }
        return documents;
    }

    public Document get(int index) {
        return toList().get(index);
    }

    public int count() {
        return toList().size();
    }

    public int size() {
        return count();
    }

}
