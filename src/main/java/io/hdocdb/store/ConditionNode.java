package io.hdocdb.store;

import org.ojai.Document;
import org.ojai.FieldPath;

import java.io.Externalizable;
import java.util.Map;
import java.util.Set;

public abstract class ConditionNode implements Externalizable {

    public abstract boolean isEmpty();

    public abstract boolean isLeaf();

    public abstract ConditionNode deepCopy();

    public abstract Set<FieldPath> getConditionPaths();

    public abstract Map<FieldPath, ConditionRange> getConditionRanges() throws IllegalStateException;

    public abstract boolean evaluate(Document document);
}
