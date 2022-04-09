package io.hdocdb.store;

import com.google.common.collect.ImmutableList;
import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.compile.*;
import io.hdocdb.execute.MutationPlan;
import io.hdocdb.execute.QueryPlan;
import io.hdocdb.util.Paths;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.DocumentExistsException;
import org.ojai.store.exceptions.DocumentNotFoundException;
import org.ojai.store.exceptions.MultiOpException;
import org.ojai.store.exceptions.StoreException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HDocumentCollection implements DocumentStore {

    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private HDocumentDB db;
    private Table table;
    private Table indexTable;
    private String family = "c";

    protected HDocumentCollection(HDocumentDB db, Table table, Table indexTable, String family) {
        this.db = db;
        this.table = table;
        this.indexTable = indexTable;
        this.family = family;
    }

    public HDocumentDB getDB() {
        return db;
    }

    protected void setDB(HDocumentDB db) {
        this.db = db;
    }

    public TableName getTableName() {
        return table.getName();
    }

    /**
     * Returns {@code true} if this Document store does not support any write
     * operations like insert/update/delete, etc.
     */
    public boolean isReadOnly() {
        return false;
    }

    /**
     * Flushes any buffered writes operations for this DocumentStore.
     *
     * @throws StoreException if the flush failed or if the flush of any
     *                        buffered operation resulted in an error.
     */
    public void flush() throws StoreException {
        // noop
    }

    public boolean isEmpty() {
        return !find(1, null, (String[])null).iterator().hasNext();
    }

    /**
     * Begins tracking the write operations performed through this instance of {@link DocumentStore}.
     *
     * @see #endTrackingWrites()
     * @see #clearTrackedWrites()
     * @see Query#waitForTrackedWrites(String)
     *
     * @throws IllegalStateException if a beginTrackingWrites() was already called
     *         and a corresponding endTrackingWrites()/clearTrackedWrites() wasn't
     */
    public void beginTrackingWrites() throws StoreException {
    }

    /**
     * Begins tracking the write operations performed through this instance of {@link DocumentStore}.
     *
     * @param previousWritesContext previously tracked writes that were retrieved from this
     *        {@link DocumentStore}, or from other {@link DocumentStore} instances. The tracking
     *        begins by using this context as the base state
     *
     * @see #endTrackingWrites()
     * @see #clearTrackedWrites()
     * @see Query#waitForTrackedWrites(String)
     *
     * @throws NullPointerException if previousWrites is {@code null}
     * @throws IllegalStateException if a beginTrackingWrites() was already called
     *         and a corresponding endTrackingWrites()/clearTrackedWrites() wasn't
     * @throws IllegalArgumentException if the specified argument can not be parsed
     */
    public void beginTrackingWrites(String previousWritesContext) throws StoreException {
    }

    /**
     * Flushes any buffered writes operations for this {@link DocumentStore} and returns a
     * writesContext which can be used to ensure that such writes are visible to ensuing queries.
     * <p/>
     * The write-context is cleared and tracking is stopped.
     * <p/>
     * This call does not isolate the writes originating from this instance of DocumentStore
     * from other instances and as a side-effect other writes issued to the same document-store
     * through other DocumentStore instances could get flushed.
     *
     * @see #beginTrackingWrites()
     * @see #clearTrackedWrites()
     * @see Query#waitForTrackedWrites(String)
     *
     * @return an encoded string representing the write-context of all writes issued,
     *         since {@link #beginTrackingWrites()} until now, through this instance of
     *         {@link DocumentStore}
     *
     * @throws StoreException if the flush failed or if the flush of any
     *         buffered operation resulted in an error.
     * @throws IllegalStateException if a corresponding {@link #beginTrackingWrites()} was not
     *         called before calling this method
     */
    public String endTrackingWrites() throws StoreException {
        return null;
    }

    /**
     * Stops the writes tracking and clears any state on this {@link DocumentStore} instance.
     * <p/>
     * This API should be called to stop tracking the writes-context in case where
     * {@link #beginTrackingWrites()} was previously called but a commit context is not needed
     * anymore, for example in case of an error in any of the mutation.
     *
     * @throws IllegalStateException if a corresponding {@link #beginTrackingWrites()} was not
     *         called before calling this method
     */
    public void clearTrackedWrites() throws StoreException {
    }

    /**
     * Returns a Document with the given {@code "_id"} field
     *
     * @param _id value to be used as the _id for this document
     * @return The Document with the given id
     * @throws StoreException
     */
    public Document findById(Value _id) throws StoreException {
        return findById(_id, (String[]) null);
    }

    public Document findById(String _id) throws StoreException {
        return findById(new HValue(_id));
    }

    /**
     * Returns a Document with the given {@code "_id"} field
     * The Document will contain only those field paths that are specified in the
     * argument. If no path parameter is specified then it returns a full document.
     *
     * @param _id value to be used as the _id for this document
     * @param paths list of fields that should be returned in the read document
     * @return The Document with the given id that can be used to requested paths.
     * @throws StoreException
     */
    public Document findById(Value _id, String... paths) throws StoreException {
        return findById(_id, true, paths);
    }

    public Document findById(String _id, String... paths) throws StoreException {
        return findById(new HValue(_id), paths);
    }

    protected Document findById(Value _id, boolean reindexArrays, String... paths) throws StoreException {
        QueryPlan plan = new QueryOneCompiler(table, family, reindexArrays, _id, null, paths).compile();
        DocumentStream stream = plan.execute();
        Iterator<Document> documents = stream.iterator();
        return documents.hasNext() ? documents.next() : null;
    }

    public Document findById(Value _id, FieldPath... paths) throws StoreException {
        return findById(_id, Paths.asPathStrings(paths));
    }

    public Document findById(String _id, FieldPath... paths) throws StoreException {
        return findById(new HValue(_id), paths);
    }

    public Document findById(Value _id, QueryCondition c) throws StoreException {
        return findById(_id, c, (String[]) null);
    }

    public Document findById(String _id, QueryCondition c) throws StoreException {
        return findById(new HValue(_id), c);
    }

    public Document findById(Value _id, QueryCondition c, String... paths) throws StoreException {
        QueryPlan plan = new QueryOneCompiler(table, family, true, _id, c, paths).compile();
        DocumentStream stream = plan.execute();
        Iterator<Document> documents = stream.iterator();
        return documents.hasNext() ? documents.next() : null;
    }

    public Document findById(String _id, QueryCondition c, String... fields) throws StoreException {
        return findById(new HValue(_id), c, fields);
    }

    public Document findById(Value _id, QueryCondition c, FieldPath... fields) throws StoreException {
        return findById(_id, c, Paths.asPathStrings(fields));
    }

    public Document findById(String _id, QueryCondition c, FieldPath... fields) throws StoreException {
        return findById(new HValue(_id), c, fields);
    }

    /**
     * Returns a DocumentStream for all the documents in the DocumentStore.
     *
     * @return A DocumentStream that can be used to retrieve all documents in the
     * this DocumentStore. The DocumentStream must be closed after
     * retrieving the documents.
     * @throws StoreException
     */
    public DocumentStream find() throws StoreException {
        return find(null, (String[]) null);
    }

    /**
     * <p>Executes the specified query on the DocumentStore and return a DocumentStream of the result.
     * <p>The returned DocumentStream must be closed after retrieving the documents.
     *
     * @return a DocumentStream that can be used to retrieve the documents in the result
     *
     * @throws StoreException
     */
    public DocumentStream findQuery(Query query) throws StoreException {
        // TODO query
        throw new UnsupportedOperationException();
    }

    /**
     * <p>Executes the specified query on the DocumentStore and return a DocumentStream of the result.
     * <p>The returned DocumentStream must be closed after retrieving the documents.
     *
     * @param queryJSON a Json string representation of OJAI Query
     * @return a DocumentStream that can be used to retrieve the documents in the result
     *
     * @throws StoreException
     */
    public DocumentStream findQuery(String queryJSON) throws StoreException {
        // TODO query
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a DocumentStream for all the documents in the DocumentStore.
     * Each Document will contain only those field paths that are specified in the
     * argument. If no path parameter is specified then it returns a full document.
     *
     * @param paths list of fields that should be returned in the read document
     * @return A DocumentStream that can be used to requested paths. The
     * DocumentStream must be closed after retrieving the documents
     * @throws StoreException
     */
    public DocumentStream find(String... paths) throws StoreException {
        return find(null, paths);
    }

    public DocumentStream find(FieldPath... paths) throws StoreException {
        return find(null, paths);
    }

    /**
     * Returns a DocumentStream with all the documents in the DocumentStore that
     * satisfies the QueryCondition.
     *
     * @param c The QueryCondition to match the documents
     * @return A DocumentStream that can be used to get documents.
     * The DocumentStream must be closed after retrieving the documents.
     */
    public DocumentStream find(QueryCondition c) throws StoreException {
        return find(c, (String[]) null);
    }

    /**
     * Returns a DocumentStream with all the documents in the DocumentStore that
     * satisfies the QueryCondition. Each Document will contain only the paths
     * that are specified in the argument.
     *
     * If no field path is specified then it returns full document for a given document.
     *
     * @param c     The QueryCondition to match the documents
     * @param paths list of fields that should be returned in the read document
     * @return A DocumentStream that can be used to read documents with requested
     * paths. The DocumentStream must be closed after retrieving the documents
     * @throws StoreException
     */
    public DocumentStream find(QueryCondition c, String... paths)
            throws StoreException {
        return getDocumentStream(null, c, paths);
    }

    public DocumentStream find(QueryCondition c, FieldPath... paths)
            throws StoreException {
        return find(c, Paths.asPathStrings(paths));
    }

    public DocumentStream find(int limit, QueryCondition c, String... paths)
            throws StoreException {
        return getDocumentStream(null, limit, c, paths);
    }

    public DocumentStream find(int limit, QueryCondition c, FieldPath... paths)
            throws StoreException {
        return find(limit, c, Paths.asPathStrings(paths));
    }

    public DocumentStream find(org.graalvm.polyglot.Value condition) {
        return find(new HQueryCondition(condition));
    }

    public DocumentStream find(org.graalvm.polyglot.Value condition,
                               org.graalvm.polyglot.Value paths) {
        return find(new HQueryCondition(condition), Paths.asPathStrings(paths));
    }

    public DocumentStream find(int limit,
                               org.graalvm.polyglot.Value condition,
                               org.graalvm.polyglot.Value paths) {
        return find(limit, new HQueryCondition(condition), Paths.asPathStrings(paths));
    }

    public DocumentStream findWithIndex(String indexName, QueryCondition c) throws StoreException {
        return findWithIndex(indexName, c, (String[]) null);
    }

    public DocumentStream findWithIndex(String indexName, QueryCondition c, String... paths)
            throws StoreException {
        return getDocumentStream(indexName, c, paths);
    }

    public DocumentStream findWithIndex(String indexName, QueryCondition c, FieldPath... paths)
            throws StoreException {
        return findWithIndex(indexName, c, Paths.asPathStrings(paths));
    }

    public DocumentStream findWithIndex(String indexName, int limit, QueryCondition c, String... paths)
            throws StoreException {
        return getDocumentStream(indexName, limit, c, paths);
    }

    public DocumentStream findWithIndex(String indexName, int limit, QueryCondition c, FieldPath... paths)
            throws StoreException {
        return findWithIndex(indexName, limit, c, Paths.asPathStrings(paths));
    }

    private DocumentStream getDocumentStream(String indexName, QueryCondition c, String... paths) {
        QueryPlan plan = new QueryCompiler(table, indexTable, family, getIndexes(), true, indexName, c, paths).compile();
        return plan.execute();
    }

    private DocumentStream getDocumentStream(String indexName, int limit, QueryCondition c, String... paths) {
        QueryPlan plan = new QueryCompiler(table, indexTable, family, getIndexes(), true, indexName, limit, c, paths).compile();
        return plan.execute();
    }

    /**
     * Inserts or replace a new document in this DocumentStore.
     *
     * The specified Document must contain an {@code "_id"} field or the operation
     * will fail.
     *
     * If the document with the given _id exists in the DocumentStore then that
     * document will be replaced by the specified document.
     *
     * @param doc The Document to be inserted or replaced in the DocumentStore.
     * @throws StoreException
     */
    public void insertOrReplace(Document doc) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getId();
        if (id == null) throw new IllegalStateException("id is null");
        insertOrReplace(id, doc);
    }

    /**
     * Inserts or replace a new document in this DocumentStore with the given _id.
     *
     * The specified document should either not contain an {@code "_id"} field or
     * its value should be same as the specified _id or the operation will fail.
     *
     * If the document with the given _id exists in the DocumentStore then that
     * document will be replaced by the specified document.
     *
     * @param doc The Document to be inserted or replaced in the DocumentStore.
     * @param _id value to be used as the _id for this document
     * @throws StoreException
     */
    public void insertOrReplace(Value _id, Document doc) throws StoreException {
        MutationPlan plan = new ReplaceCompiler(table, indexTable, family, getIndexes(), _id, doc, null).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not insert or replace, it may have changed: " + _id);
        }
    }

    public void insertOrReplace(String _id, Document doc) throws StoreException {
        insertOrReplace(new HValue(_id), doc);
    }

    /**
     * Inserts or replace a new document in this DocumentStore with the value of
     * the specified Field as the {@code _id}.
     *
     * If the document with the given _id exists in the DocumentStore then that
     * document will be replaced by the specified document.
     *
     * @param doc        The Document to be inserted or replaced in the DocumentStore.
     * @param fieldAsKey document's field to be used as the key when an id is not
     *                   passed in and the document doesn't have an "_id" field or
     *                   a different field is desired to be used as _id.
     * @throws StoreException
     */
    public void insertOrReplace(Document doc, FieldPath fieldAsKey) throws StoreException {
        insertOrReplace(doc, fieldAsKey.asPathString());
    }

    /**
     * Inserts or replace a new document in this DocumentStore with the value of
     * the specified Field as the {@code _id}.
     *
     * If the document with the given _id exists in the DocumentStore then that
     * document will be replaced by the specified document.
     *
     * @param doc        The Document to be inserted or replaced in the DocumentStore.
     * @param fieldAsKey document's field to be used as the key when an id is not
     *                   passed in and the document doesn't have an "_id" field or
     *                   a different field is desired to be used as _id.
     * @throws StoreException
     */
    public void insertOrReplace(Document doc, String fieldAsKey) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getValue(fieldAsKey);
        if (id == null) throw new IllegalStateException("id is null");
        insertOrReplace(id, doc);
    }

    public void insertOrReplace(org.graalvm.polyglot.Value json) {
        HValue value = HValue.initFromObject(json);
        if (value.getType() != Value.Type.MAP) {
            throw new StoreException("JSON not of type MAP");
        }
        Document doc = (HDocument) value;
        insertOrReplace(doc);
    }

    private Value getKeyField(Document doc, String fieldAsKey) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        if (fieldAsKey == null) throw new IllegalArgumentException("fieldAsKey is null");
        Value value = doc.getValue(fieldAsKey);
        if (value == null) throw new IllegalArgumentException("key's value is null");
        return value;
    }

    /**
     * Inserts all documents from the specified DocumentStream into this DocumentStore.
     *
     * This is a synchronous API and it won't return until all the documents
     * in the DocumentStream are written to the DocumentStore or some error has
     * occurred while storing the documents. Each document read from the DocumentStream
     * must have a field "_id"; otherwise, the operation will fail.
     *
     * If there is an error in reading from the stream or in writing to the DocumentStore
     * then a MultiOpException will be thrown containing the list of documents that
     * failed to be stored in the DocumentStore. Reading from a stream stops on the
     * first read error. If only write errors occur, the iterator will stop and the
     * rest of the documents will remain un-consumed in the DocumentStream.
     *
     * If the parameter {@code fieldAsKey} is provided, will be stored as the "_id"
     * of the stored document. If an "_id" field is present in the documents, an
     * error will be thrown. When reading the document back from the store, the
     * key will be returned back as usual as the "_id" field.
     *
     * @param stream     The DocumentStream to read the documents from.
     * @throws MultiOpException which has a list of write-failed documents and
     *                          their errors.
     */
    public void insertOrReplace(DocumentStream stream) throws MultiOpException {
        insertOrReplace(stream, (String) null);
    }

    public void insertOrReplace(DocumentStream stream, FieldPath fieldAsKey)
            throws MultiOpException {
        insertOrReplace(stream, fieldAsKey.asPathString());
    }

    public void insertOrReplace(DocumentStream stream, String fieldAsKey)
            throws MultiOpException {
        throw new UnsupportedOperationException();
    }

    /**
     * Applies a mutation on the document identified by the document id.
     *
     * All updates specified by the mutation object should be applied atomically,
     * and consistently meaning either all of the updates in mutation are applied
     * or none of them is applied and a partial update should not be visible to an
     * observer.
     *
     * @param _id document id
     * @param m   a mutation object specifying the mutation operations on the document
     * @throws StoreException
     */
    public void update(Value _id, DocumentMutation m) throws StoreException {
        Document doc = ((HDocumentMutation)m).isReadModifyWrite() ? findById(_id, false) : null;
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void update(String _id, DocumentMutation m) throws StoreException {
        update(new HValue(_id), m);
    }

    public void update(org.graalvm.polyglot.Value condition, org.graalvm.polyglot.Value m) throws StoreException {
        update(condition, m, false);
    }

    public void update(org.graalvm.polyglot.Value condition,
                       org.graalvm.polyglot.Value m,
                       boolean multi) throws StoreException {
        HQueryCondition c = new HQueryCondition(condition);
        HDocumentMutation mutation = new HDocumentMutation(m);
        DocumentStream stream = find(c);
        for (Document doc : stream) {
            MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), doc.getId(), mutation, doc).compile();
            plan.execute();
            if (!multi) break;
        }
    }

    /**
     * Deletes a document with the given id. This operation is successful even
     * when the document with the given id doesn't exist.
     *
     * If the parameter {@code fieldAsKey} is provided, its value will be used as
     * the "_id" to delete the document.
     *
     * @param _id        document id
     * @throws StoreException
     */
    public void delete(Value _id) throws StoreException {
        MutationPlan plan = new DeleteCompiler(table, family, _id, null).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not delete, it may have changed: " + _id);
        }
    }

    public void delete(String _id) throws StoreException {
        delete(new HValue(_id));
    }

    public void delete(Document doc) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getId();
        if (id == null) throw new IllegalStateException("id is null");
        delete(id);
    }

    public void delete(Document doc, FieldPath fieldAsKey) throws StoreException {
        delete(doc, fieldAsKey.asPathString());
    }

    public void delete(Document doc, String fieldAsKey) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getValue(fieldAsKey);
        if (id == null) throw new IllegalStateException("id is null");
        delete(id);
    }

    /**
     * Deletes a set of documents from the DocumentStore represented by the DocumentStream.
     * This is a synchronous API and it won't return until all the documents
     * in the DocumentStream are written to the DocumentStore or some error occurs while
     * writing the documents. Each document read from the DocumentStream must have a
     * field "_id" of type BINARY or UTF8-string; otherwise the operation will fail.
     *
     * If there is an error in reading from the stream or in writing to
     * the DocumentStore then a MultiOpException will be thrown that contains a list of
     * documents that failed to write to the DocumentStore. Reading from a stream stops on
     * the first read error. If only write errors occur, the iterator will stop
     * and the current list of failed document is returned in a multi op exception.
     * The untouched documents will remain in the DocumentStream.
     *
     * @param stream     DocumentStream
     * @throws MultiOpException which has a list of write-failed documents and
     *                          their errors
     */
    public void delete(DocumentStream stream) throws MultiOpException {
        delete(stream, (String) null);
    }

    public void delete(DocumentStream stream, FieldPath fieldAsKey)
            throws MultiOpException {
        delete(stream, fieldAsKey.asPathString());
    }

    public void delete(DocumentStream stream, String fieldAsKey)
            throws MultiOpException {
        throw new UnsupportedOperationException();
    }

    /**
     * Inserts a document with the given id. This operation is successful only
     * when the document with the given id doesn't exist.
     *
     * "fieldAsKey", when provided, will also be stored as the "_id" field in the
     * written document for the document. If "_id" already existed in the document, then
     * an error will be thrown. When reading the document back from the DB, the
     * key will be returned back as usual as the "_id" field.
     *
     * If the passed in id is not a direct byte buffer, there will be a copy to one.
     *
     * Note that an insertOrReplace() operation would be more efficient than an
     * insert() call.
     *
     * @param doc        JSON document as the new value for the given document
     * @param _id        to be used as the key for the document
     * @throws DocumentExistsException when a document with id already exists in DocumentStore
     */
    public void insert(Value _id, Document doc) throws StoreException {
        MutationPlan plan = new InsertCompiler(table, indexTable, family, getIndexes(), _id, doc).compile();
        if (!plan.execute()) {
            throw new DocumentExistsException("Could not insert: " + _id);
        }
    }

    public void insert(String _id, Document doc) throws StoreException {
        insert(new HValue(_id), doc);
    }

    public void insert(Document doc) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getId();
        if (id == null) throw new IllegalStateException("id is null");
        insert(id, doc);
    }

    public void insert(Document doc, FieldPath fieldAsKey) throws StoreException {
        insert(doc, fieldAsKey.asPathString());

    }

    public void insert(Document doc, String fieldAsKey) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getValue(fieldAsKey);
        if (id == null) throw new IllegalStateException("id is null");
        insert(id, doc);
    }

    public void insert(org.graalvm.polyglot.Value json) {
        HValue value = HValue.initFromObject(json);
        if (value.getType() != Value.Type.MAP) {
            throw new StoreException("JSON not of type MAP");
        }
        Document doc = (HDocument) value;
        insert(doc);
    }

    /**
     * Inserts a set of documents represented by the DocumentStream into the DocumentStore.
     * This is a synchronous API that won't return until all the documents
     * in the DocumentStream are written to the DocumentStore or some error occurs while
     * writing the documents. Each document read from the DocumentStream must have a
     * field "_id" of type BINARY or UTF8-string; otherwise, the operation will
     * fail or it will be of the Document type.
     *
     * If a document with the given key exists on the server then it throws a document
     * exists exception, similar to the non-DocumentStream based insert() API.
     *
     * If there is an error in reading from the stream or in writing to
     * the DocumentStore then a MultiOpException will be thrown that contains a list of
     * documents that failed to write to the DocumentStore. Reading from a stream stops on
     * the first read error. If only write errors occur, the iterator will stop
     * and the current list of failed document is returned in a multi op exception.
     * The untouched documents will remain in the DocumentStream.
     *
     * @param stream     DocumentStream
     * @throws MultiOpException which has a list of write-failed documents and
     *                          their errors
     */
    public void insert(DocumentStream stream) throws MultiOpException {
        insert(stream, (String) null);
    }

    public void insert(DocumentStream stream, FieldPath fieldAsKey)
            throws MultiOpException {
        insert(stream, fieldAsKey.asPathString());
    }

    public void insert(DocumentStream stream, String fieldAsKey)
            throws MultiOpException {
        throw new UnsupportedOperationException();
    }

    /**
     * Replaces a document in the DocumentStore. The document id is either explicitly specified
     * as parameter "id" or it is implicitly specified as the field "_id" in the
     * passed document. If the document id is explicitly passed then the document should
     * not contain "_id" field or its value should be the same as the explicitly
     * specified id; otherwise, the operation will  fail.
     *
     * If the document with the given key does not exist on the server then it will
     * throw DocumentNotFoundException.
     *
     * "fieldAsKey", when provided, will also be stored as the "_id" field in the
     * written document for the document. If "_id" already existed in the document, then
     * an error will be thrown. When reading the document back from the DB, the
     * key would be returned back as usual as "_id" field.
     *
     * If the passed in id is not a direct byte buffer, there will be a copy to one.
     *
     * Note that an insertOrReplace() operation would be more efficient than an
     * replace() call.
     *
     * @param doc        JSON document as the new value for the given document
     * @param _id        to be used as the key for the document
     * @throws DocumentNotFoundException when a document with the id does not exist in DocumentStore
     */
    public void replace(Value _id, Document doc) throws StoreException {
        MutationPlan plan = new ReplaceCompiler(table, indexTable, family, getIndexes(), _id, doc, new HDocument()).compile();
        if (!plan.execute()) {
            throw new DocumentNotFoundException("Could not replace: " + _id);
        }
    }

    public void replace(String _id, Document doc) throws StoreException {
        replace(new HValue(_id), doc);
    }

    public void replace(Document doc) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getId();
        if (id == null) throw new IllegalStateException("id is null");
        replace(id, doc);
    }

    public void replace(Document doc, FieldPath fieldAsKey) throws StoreException {
        replace(doc, fieldAsKey.asPathString());
    }

    public void replace(Document doc, String fieldAsKey) throws StoreException {
        if (doc == null) throw new IllegalArgumentException("doc is null");
        Value id = doc.getValue(fieldAsKey);
        if (id == null) throw new IllegalStateException("id is null");
        replace(id, doc);
    }

    public void replace(org.graalvm.polyglot.Value json) throws StoreException {
        HValue value = HValue.initFromObject(json);
        if (value.getType() != Value.Type.MAP) {
            throw new StoreException("JSON not of type MAP");
        }
        Document doc = (HDocument) value;
        replace(doc);
    }

    /**
     * Replaces a set of documents represented by the DocumentStream into the DocumentStore.
     * This is a synchronous API and it won't return until all the documents
     * in the DocumentStream are written to the DocumentStore or some error occurs while
     * writing the documents. Each document read from the DocumentStream must have a
     * field "_id" of type BINARY or UTF8-string; otherwise, the operation will
     * fail or it will be of Document type.
     *
     * If the document with the given key does not exist on the server then it throws,
     * a document not exists exception, similar to the non-DocumentStream based
     * replace() API.
     *
     * If there is an error in reading from the stream or in writing to
     * the DocumentStore then a MultiOpException will be thrown that contains a list of
     * documents that failed to write to the DocumentStore. Reading from a stream stops on
     * the first read error. If only write errors occur, the iterator will stop
     * and the current list of failed document is returned in a multi op exception.
     * The untouched documents will remain in the DocumentStream.
     *
     * @param stream     A DocumentStream to read the documents from
     * @throws MultiOpException which has list of write-failed documents and
     *                          their errors
     */
    public void replace(DocumentStream stream) throws MultiOpException {
        replace(stream, (String) null);
    }

    public void replace(DocumentStream stream, FieldPath fieldAsKey)
            throws MultiOpException {
        replace(stream, fieldAsKey.asPathString());

    }

    public void replace(DocumentStream stream, String fieldAsKey)
            throws MultiOpException {
        throw new UnsupportedOperationException();

    }

    /**
     * Atomically applies an increment to a given field (in dot separated notation)
     * of the given document id. If the field doesn't exist on the server
     * then it will be created with the type of the incremental value.
     * The increment operation can be applied on any of the numeric
     * types, such as byte, short, int, long, float, double, or decimal,
     * of a field. The operation will fail if the increment is applied to a
     * field that is of a non-numeric type.
     *
     * If an id doesn't exist, it gets created (similar to the insertOrReplace
     * behavior). And it is created, with the value of 'inc' parameter. The same
     * logic applies to intermittent paths in the path: they get created top to
     * bottom.
     *
     * If the type is different than the field in the operation, it fails.
     *
     * The increment operation won't change the type of existing value stored in
     * the given field for the document. The resultant value of the field will be
     * truncated based on the original type of the field.
     *
     * For example, if a field 'score' is of type int and contains 60 and an
     * increment of double '5.675' is applied, then the resultant value of the
     * field will be 65 (65.675 will be truncated to 65).
     *
     * If the type to which the increment is applied is a byte, short, or int,
     * then it needs to use long as the operation.
     *
     * If the passed in id is not a direct byte buffer, there will be a copy to one.
     *
     * @param _id   document id
     * @param field the field name in dot separated notation
     * @param inc   increment to apply to a field. Can be positive or negative
     * @throws StoreException
     */
    public void increment(Value _id, String field, byte inc) throws StoreException {
        Document doc = findById(_id, false, field);
        DocumentMutation m = new HDocumentMutation().increment(field, inc);
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void increment(String _id, String field, byte inc) throws StoreException {
        increment(new HValue(_id), field, inc);
    }

    public void increment(Value _id, String field, short inc) throws StoreException {
        Document doc = findById(_id, false, field);
        DocumentMutation m = new HDocumentMutation().increment(field, inc);
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void increment(String _id, String field, short inc) throws StoreException {
        increment(new HValue(_id), field, inc);
    }

    public void increment(Value _id, String field, int inc) throws StoreException {
        Document doc = findById(_id, false, field);
        DocumentMutation m = new HDocumentMutation().increment(field, inc);
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void increment(String _id, String field, int inc) throws StoreException {
        increment(new HValue(_id), field, inc);
    }

    public void increment(Value _id, String field, long inc) throws StoreException {
        Document doc = findById(_id, false, field);
        DocumentMutation m = new HDocumentMutation().increment(field, inc);
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void increment(String _id, String field, long inc) throws StoreException {
        increment(new HValue(_id), field, inc);
    }

    public void increment(Value _id, String field, float inc) throws StoreException {
        Document doc = findById(_id, false, field);
        DocumentMutation m = new HDocumentMutation().increment(field, inc);
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void increment(String _id, String field, float inc) throws StoreException {
        increment(new HValue(_id), field, inc);
    }

    public void increment(Value _id, String field, double inc) throws StoreException {
        Document doc = findById(_id, false, field);
        DocumentMutation m = new HDocumentMutation().increment(field, inc);
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void increment(String _id, String field, double inc) throws StoreException {
        increment(new HValue(_id), field, inc);
    }

    public void increment(Value _id, String field, BigDecimal inc) throws StoreException {
        Document doc = findById(_id, false, field);
        DocumentMutation m = new HDocumentMutation().increment(field, inc);
        MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
        if (!plan.execute()) {
            throw new StoreException("Could not update, it may have changed: " + _id);
        }
    }

    public void increment(String _id, String field, BigDecimal inc) throws StoreException {
        increment(new HValue(_id), field, inc);
    }

    /**
     * Atomically evaluates the condition on a given document and if the
     * condition holds true for the document then a mutation is applied on the document.
     *
     * If an id doesn't exist, the function returns false (no exception is thrown).
     * If the mutation operation fails, it throws exception.
     *
     * If the passed in id is not a direct byte buffer, there will be a copy to one.
     *
     * @param _id       document id
     * @param condition the condition to evaluate on the document
     * @param m         mutation to apply on the document
     * @return True if the condition is true for the document; otherwise, false
     * @throws StoreException if the condition passes but the mutate fails
     */
    public boolean checkAndMutate(Value _id, QueryCondition condition, DocumentMutation m) throws StoreException {
        Document doc = findById(_id, false);
        if (((HQueryCondition)condition).evaluate(doc)) {
            MutationPlan plan = new UpdateCompiler(table, indexTable, family, getIndexes(), _id, m, doc).compile();
            return plan.execute();
        }
        return false;
    }

    public boolean checkAndMutate(String _id, QueryCondition condition, DocumentMutation m) throws StoreException {
        return checkAndMutate(new HValue(_id), condition, m);
    }

    public boolean checkAndMutate(String _id,
                                  org.graalvm.polyglot.Value condition,
                                  org.graalvm.polyglot.Value m) throws StoreException {
        return checkAndMutate(_id, new HQueryCondition(condition), new HDocumentMutation(m));
    }

    /**
     * Atomically evaluates the condition on given document and if the
     * condition holds true for the document then it is atomically deleted.
     *
     * If id doesnt exist, returns false (no exception is thrown).
     * If deletion operation fails, it throws exception.
     *
     * If the passed in id is not a direct byte buffer, there will be a copy to one.
     *
     * @param _id       document id
     * @param condition condition to evaluate on the document
     * @return True if the condition is valid for the document, otherwise false.
     * @throws StoreException if the condition passes but the delete fails
     */
    public boolean checkAndDelete(Value _id, QueryCondition condition) throws StoreException {
        Document doc = findById(_id, false);
        if (((HQueryCondition)condition).evaluate(doc)) {
            MutationPlan plan = new DeleteCompiler(table, family, _id, doc).compile();
            return plan.execute();
        }
        return false;
    }

    public boolean checkAndDelete(String _id, QueryCondition condition) throws StoreException {
        return checkAndDelete(new HValue(_id), condition);
    }

    public boolean checkAndDelete(String _id, org.graalvm.polyglot.Value condition) throws StoreException {
        return checkAndDelete(_id, new HQueryCondition(condition));
    }

    /**
     * Atomically evaluates the condition on the document with the given id and if the
     * condition holds true for the document then it atomically replaces the document
     * with the given document.
     *
     * If the id doesn't exist, the function returns false (no exception is thrown).
     * If the replace operation fails, it throws an exception.
     *
     * If the passed in id is not a direct byte buffer, there will be a copy to one.
     *
     * @param _id       document id
     * @param condition the condition to evaluate on the document
     * @param doc       document to replace
     * @return True if the condition is true for the document otherwise false
     * @throws StoreException if the condition passes but the replace fails
     */
    public boolean checkAndReplace(Value _id, QueryCondition condition, Document doc) throws StoreException {
        Document oldDoc = findById(_id, false);
        if (((HQueryCondition)condition).evaluate(oldDoc)) {
            MutationPlan plan = new ReplaceCompiler(table, indexTable, family, getIndexes(), _id, doc, oldDoc).compile();
            return plan.execute();
        }
        return false;
    }

    public boolean checkAndReplace(String _id, QueryCondition condition, Document doc) throws StoreException {
        return checkAndReplace(new HValue(_id), condition, doc);
    }

    public boolean checkAndReplace(String _id,
                                   org.graalvm.polyglot.Value condition,
                                   org.graalvm.polyglot.Value json) throws StoreException {
        HValue value = HValue.initFromObject(json);
        if (value.getType() != Value.Type.MAP) {
            throw new StoreException("JSON not of type MAP");
        }
        Document doc = (HDocument) value;
        return checkAndReplace(_id, new HQueryCondition(condition), doc);
    }

    public IndexBuilder newIndexBuilder(String name) {
        return new IndexBuilder(db, this, table, name);
    }

    public Index createIndex(String name, String path, Value.Type type) {
        return createIndex(name, path, type, Order.ASCENDING);
    }

    public Index createIndex(String name, String path, Value.Type type, Order order) {
        return createIndex(name, path, type, order, true);
    }

    public Index createIndex(String name, String path, Value.Type type, Order order, boolean async) {
        return newIndexBuilder(name)
                .add(path, type, order)
                .setAsync(async)
                .build();
    }

    public Index getIndex(String name) {
        if (indexTable == null) return null;
        return getDB().getIndexes(table.getName()).get(name);
    }

    public Collection<Index> getIndexes() {
        if (indexTable == null) return Collections.emptyList();
        return getDB().getIndexes(table.getName()).values();
    }

    public int getIndexSize(String name) {
        return new QueryIndexCompiler(indexTable, getIndex(name), null, null).compile().execute().size();
    }

    public void populateIndex(String name) {
        populateIndex(name, true);
    }

    public void populateIndex(String name, boolean async) {
        Index index = getIndex(name);
        if (index == null) return;

        // first set to BUILDING
        getDB().updateIndexState(table.getName(), name, Index.State.BUILDING);

        Runnable runnable = new PopulateIndexCommand(index);
        if (async) {
            executor.schedule(runnable, HDocumentDB.INDEX_STATE_CHANGE_DELAY_SECS, TimeUnit.SECONDS);
        } else {
            runnable.run();
        }
    }

    class PopulateIndexCommand implements Runnable {
        Index index;
        public PopulateIndexCommand(Index index) {
            this.index = index;
        }
        public void run() {
            DocumentStream documentStream = find();
            for (Document doc : documentStream) {
                MutationPlan plan = new InsertIndexCompiler(indexTable, ImmutableList.of(index), doc.getId(), doc).compile();
                plan.execute();
            }
            getDB().updateIndexState(table.getName(), index.getName(), Index.State.ACTIVE);
        }
    }

    public void dropIndex(String name) {
        dropIndex(name, true);
    }

    public void dropIndex(String name, boolean async) {
        Index index = getIndex(name);
        if (index == null) return;

        // first set to INACTIVE so index entries are no longer added
        getDB().updateIndexState(table.getName(), name, Index.State.INACTIVE);

        Runnable runnable = new DropIndexCommand(index);
        if (async) {
            executor.schedule(runnable, HDocumentDB.INDEX_STATE_CHANGE_DELAY_SECS, TimeUnit.SECONDS);
        } else {
            runnable.run();
        }
    }

    class DropIndexCommand implements Runnable {
        Index index;
        public DropIndexCommand(Index index) {
            this.index = index;
        }
        public void run() {
            MutationPlan plan = new DeleteIndexCompiler(indexTable, index).compile();
            plan.execute();
            getDB().updateIndexState(table.getName(), index.getName(), Index.State.DROPPED);
        }
    }

    /**
     * Override {@link AutoCloseable#close()} to avoid declaring a checked exception.
     */
    public void close() throws StoreException {
        try {
            table.close();
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    /*
     * The following methods are for Nashorn integration
     */

    public Document findOne() throws StoreException {
        return findOne((HQueryCondition)null);
    }

    public Document findOne(HQueryCondition condition) throws StoreException {
        return findOne(condition, (String[])null);
    }

    public Document findOne(org.graalvm.polyglot.Value condition) throws StoreException {
        return findOne(new HQueryCondition(condition));
    }

    public Document findOne(HQueryCondition condition, String... paths) {
        Iterator<Document> iter = find(1, condition, paths).iterator();
        return iter.hasNext() ? iter.next() : null;
    }

    public Document findOne(org.graalvm.polyglot.Value condition,
                            org.graalvm.polyglot.Value paths) {
        return findOne(new HQueryCondition(condition), Paths.asPathStrings(paths));
    }

    public void save(Document document) {
        if (document.getId() == null) {
            document.setId(UUID.randomUUID().toString());
        }
        insertOrReplace(document);
    }

    public void save(org.graalvm.polyglot.Value json) {
        if (!json.hasMember("_id")) {
            json.putMember("_id", UUID.randomUUID().toString());
        }
        insertOrReplace(json);
    }

    public void remove(String id) {
        delete(id);
    }

    public void drop() {
        getDB().dropCollection(getTableName());
    }
}
