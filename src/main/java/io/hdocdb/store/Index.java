package io.hdocdb.store;

import com.google.common.collect.Lists;
import io.hdocdb.HDocument;
import io.hdocdb.HList;
import io.hdocdb.HValue;
import org.ojai.Document;
import org.ojai.Value;
import org.ojai.store.exceptions.StoreException;

import java.util.List;

public class Index {

    public static final String NONE = "_none_";
    public static final String DEFAULT_FAMILY = "c";
    public static final String NAME_PATH = "name";
    public static final String STATE_PATH = "state";
    public static final String FIELDS_PATH = "fields";

    public enum State {
        CREATED,
        BUILDING,
        ACTIVE,
        INACTIVE, // an intermediate state to wait for clients to stop using an index
        DROPPED
    }

    private final String name;
    private final List<IndexFieldPath> elements;
    private State state;

    public Index(String name, List<IndexFieldPath> elements) {
        this.name = name;
        this.elements = elements;
        this.state = State.CREATED;
    }

    public Index(Document document) {
        this.name = document.getString(NAME_PATH);
        this.elements = Lists.newArrayList();
        Value fields = document.getValue(FIELDS_PATH);
        if (fields.getType() != Value.Type.ARRAY) {
            throw new StoreException("Invalid index document");
        }
        for (HValue field : ((HList) fields).getHValues()) {
            if (field.getType() != Value.Type.MAP) {
                throw new StoreException("Invalid index document");
            }
            IndexFieldPath path = new IndexFieldPath((Document) field);
            this.elements.add(path);
        }
        this.state = State.valueOf(document.getString(STATE_PATH));
    }

    public Document asDocument() {
        HDocument doc = new HDocument();
        doc.set(NAME_PATH, name);
        doc.set(STATE_PATH, state.toString());
        List<Object> fields = new HList();
        int i = 0;
        for (IndexFieldPath field : elements) {
            fields.set(i++, field.asDocument());
        }
        doc.set(FIELDS_PATH, fields);
        return doc;
    }

    public String getName() {
        return name;
    }

    public int size() {
        return elements.size();
    }

    public IndexFieldPath getField(int index) {
        return elements.get(index);
    }

    public List<IndexFieldPath> getFields() {
        return elements;
    }

    public List<String> getPaths() {
        List<String> paths = Lists.newArrayList();
        for (IndexFieldPath element : elements) {
            paths.add(element.getPath().asPathString());
        }
        return paths;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Index index = (Index) o;

        if (!name.equals(index.name)) return false;
        if (!elements.equals(index.elements)) return false;
        return state == index.state;

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + elements.hashCode();
        result = 31 * result + state.hashCode();
        return result;
    }
}
