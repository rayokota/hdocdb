package io.hdocdb;

import com.google.common.collect.Lists;
import io.hdocdb.store.ConditionLeaf;
import io.hdocdb.util.FieldSegmentIterator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.ojai.FieldPath;
import org.ojai.FieldSegment;
import org.ojai.store.exceptions.StoreException;

import java.util.*;

public class HList extends HContainer implements List<Object> {

    private Map<Integer, HValue> elements = new TreeMap<>();
    private boolean compareValueTimestamps = false;

    public HList() {
        setType(Type.ARRAY);
    }

    public int size() {
        return elements.size();
    }

    public boolean isEmpty() {
        return elements.isEmpty();
    }

    public boolean contains(Object o) {
        return elements.containsValue(HValue.initFromObject(o));
    }

    public Iterator<Object> iterator() {
        final Iterator<HValue> itr = elements.values().iterator();
        return new Iterator<>() {
            public void remove() {
                throw new UnsupportedOperationException();
            }

            public Object next() {
                return itr.next().getObject();
            }

            public boolean hasNext() {
                return itr.hasNext();
            }
        };
    }

    public Object[] toArray() {
        ArrayList<Object> objs = new ArrayList<>(elements.size());

        for (HValue kv : elements.values()) {
            objs.add(kv.getObject());
        }

        return objs.toArray();
    }

    @SuppressWarnings("unchecked")
    public Object[] toArray(Object[] a) {
        return a;
    }

    public boolean add(Object e) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(Collection c) {
        Iterator itr = c.iterator();

        Object o;
        do {
            if (!itr.hasNext()) {
                return true;
            }

            o = itr.next();
        } while (this.elements.containsValue(HValue.initFromObject(o)));

        return false;
    }

    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(int index, Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public Object get(int index) {
        HValue value = elements.get(index);
        return value != null ? value.getObject() : null;
    }

    public HValue getHValue(int index) {
        return elements.get(index);
    }

    public Object set(int index, Object element) {
        Object oldObject = get(index);
        HValue value = HValue.initFromObject(element);
        elements.put(index, value);
        return oldObject;
    }

    public void add(int index, Object element) {
        throw new UnsupportedOperationException();
    }

    public Object remove(int index) {
        throw new UnsupportedOperationException();
    }

    public int indexOf(Object o) {
        return Arrays.asList(toArray()).indexOf(HValue.initFromObject(o));
    }

    public int lastIndexOf(Object o) {
        return Arrays.asList(toArray()).lastIndexOf(HValue.initFromObject(o));
    }

    public ListIterator<Object> listIterator() {
        return new HListIterator();
    }

    public ListIterator<Object> listIterator(int index) {
        return new HListIterator(index);
    }

    public List<Object> subList(int fromIndex, int toIndex) {
        return Arrays.asList(toArray()).subList(fromIndex, toIndex);
    }

    public boolean evaluate(ConditionLeaf.CompareOp op, HValue value) {
        switch (op) {
            case NONE:
                return true;
            case EQ:
            case NE:
                // separate this from comparison as compare ops cannot handle mixed types
                boolean equals = value.getType() == Type.ARRAY ? this.equals(value) : this.contains(value);
                return op == ConditionLeaf.CompareOp.EQ && equals || op == ConditionLeaf.CompareOp.NE && !equals;
            case LT:
            case LE:
            case GE:
            case GT:
                // use "any" semantics (rather than "all" semantics)
                for (HValue element : elements.values()) {
                    try {
                        int compare = element.compareTo(value);
                        if (op == ConditionLeaf.CompareOp.LT && compare < 0
                                || op == ConditionLeaf.CompareOp.LE && compare <= 0
                                || op == ConditionLeaf.CompareOp.GE && compare >= 0
                                || op == ConditionLeaf.CompareOp.GT && compare > 0) {
                            return true;
                        }
                    } catch (IllegalArgumentException e) {
                        continue;
                    }
                }
                return false;
            default:
                throw new IllegalArgumentException("[] not supported with op " + op);
        }
    }

    public HList shallowCopy() {
        HList result = new HList();
        result.elements = this.elements;
        result.compareValueTimestamps = this.compareValueTimestamps;
        result.type = this.type;
        result.value = this.value;
        return result;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean first = true;
        for (HValue value : elements.values()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            if (value != null) {
                sb.append(value);
            } else {
                sb.append("null");
            }
        }
        return sb.append(']').toString();
    }

    public Collection<HValue> getHValues() {
        return elements.values();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HList objects = (HList) o;

        return elements.equals(objects.elements);
    }

    @Override
    public int hashCode() {
        return elements.hashCode();
    }

    public HValue checkHValue(Iterator<FieldSegment> path, HValue value) throws StoreException {
        FieldSegment field = path.next();
        if (field == null) return null;
        int index = field.getIndexSegment().getIndex();
        if (index < 0 || index > size()) {
            throw new StoreException("Path index " + index + " is out of bounds");
        }
        HValue oldValue = elements.get(index);
        if (oldValue == null) return null;
        if (field.isLastPath()) {
            if (!oldValue.isSettableWithType(value.getType())) {
                throw new StoreException("New type " + value.getType() + " does not match existing type " + oldValue.getType());
            }
            return oldValue;
        } else if (field.isMap()) {
            if (!oldValue.isSettableWithType(Type.MAP)) {
                throw new StoreException("Map " + field + " does not match existing type " + oldValue.getType());
            }
            HDocument doc = (HDocument) oldValue;
            return doc.checkHValue(path, value);
        } else if (field.isArray()) {
            if (!oldValue.isSettableWithType(Type.ARRAY)) {
                throw new StoreException("Array " + field + " does not match existing type " + oldValue.getType());
            }
            HList list = (HList) oldValue;
            return list.checkHValue(path, value);
        }
        return null;
    }

    public HValue getHValue(Iterator<FieldSegment> path) {
        FieldSegment field = path.next();
        if (field == null) return null;
        int index = field.getIndexSegment().getIndex();
        if (index == -1) {
            if (field.isLastPath()) {
                return this;  // handle [], return this for []
            } else if (field.isMap()) {
                HList result = new HList();
                int i = 0;
                FieldSegment childField = path.next();
                for (HValue value : getHValues()) {
                    if (value.getType() != Type.MAP) return null;
                    HDocument doc = (HDocument) value;
                    result.set(i++, doc.getHValue(new FieldSegmentIterator(childField)));
                }
                return result;
            } else if (field.isArray()) {
                HList result = new HList();
                int i = 0;
                FieldSegment childField = path.next();
                for (HValue value : getHValues()) {
                    if (value.getType() != Type.ARRAY) return null;
                    HList list = (HList) value;
                    HValue subvalue = list.getHValue(new FieldSegmentIterator(childField));
                    if (subvalue.getType() != Type.ARRAY) {
                        result.set(i++, subvalue);
                    } else {
                        // perform a "flatmap"
                        HList sublist = (HList) subvalue;
                        for (HValue v : sublist.getHValues()) {
                            result.set(i++, v);
                        }
                    }
                }
                return result;
            }
        } else {
            HValue value = elements.get(index);
            if (value == null || field.isLastPath()) {
                return value;
            } else if (field.isMap()) {
                if (value.getType() != Type.MAP) return null;
                HDocument doc = (HDocument) value;
                return doc.getHValue(path);
            } else if (field.isArray()) {
                if (value.getType() != Type.ARRAY) return null;
                HList list = (HList) value;
                return list.getHValue(path);

            }
        }
        return null;
    }

    public void setHValue(Iterator<FieldSegment> path, HValue value) {
        FieldSegment field = path.next();
        if (field == null) return;
        int index = field.getIndexSegment().getIndex();
        if (index == -1) return;
        HValue oldValue = elements.get(index);
        if (field.isLastPath()) {
            if (compareValueTimestamps && value.hasNewerTs(oldValue)) return;
            elements.put(index, value);
        } else if (field.isMap()) {
            if (oldValue != null && oldValue.getType() == Type.MAP) {
                HDocument doc = (HDocument)oldValue;
                doc.setCompareValueTimestamps(compareValueTimestamps);
                doc.setHValue(path, value);
            } else {
                if (compareValueTimestamps && value.hasNewerTs(oldValue)) return;
                HDocument doc = new HDocument();
                doc.setCompareValueTimestamps(compareValueTimestamps);
                doc.setHValue(path, value);
                elements.put(index, doc);
            }
        } else if (field.isArray()) {
            if (oldValue != null && oldValue.getType() == Type.ARRAY) {
                HList list = (HList) oldValue;
                list.setCompareValueTimestamps(compareValueTimestamps);
                list.setHValue(path, value);
            } else {
                if (compareValueTimestamps && value.hasNewerTs(oldValue)) return;
                HList list = new HList();
                list.setCompareValueTimestamps(compareValueTimestamps);
                list.setHValue(path, value);
                elements.put(index, list);
            }
        }
    }

    public void removeHValue(Iterator<FieldSegment> path) {
        FieldSegment field = path.next();
        if (field == null) return;
        int index = field.getIndexSegment().getIndex();
        if (index == -1) return;
        HValue oldValue = elements.get(index);
        if (oldValue == null) return;
        if (field.isLastPath()) {
            elements.remove(index);
        } else if (field.isMap()) {
            if (oldValue.getType() == Type.MAP) {
                HDocument doc = (HDocument) oldValue;
                doc.removeHValue(path);
            }
        } else if (field.isArray()) {
            if (oldValue.getType() == Type.ARRAY) {
                HList list = (HList) oldValue;
                list.removeHValue(path);
            }
        }
    }

    public void clearHValues() {
        // don't call elements.clear() due to possible sharing from shallowCopy()
        elements = new TreeMap<>();
    }

    protected boolean compareValueTimestamps() {
        return compareValueTimestamps;
    }

    protected void setCompareValueTimestamps(boolean compare) {
        this.compareValueTimestamps = compare;
    }

    protected HList reindexArrays() {
        HList list = shallowCopy();
        list.clear();
        int index = 0;
        for (HValue value : getHValues()) {
            if (value.getType() == Type.MAP) {
                value = ((HDocument)value).reindexArrays();
            }
            else if (value.getType() == Type.ARRAY) {
                value = ((HList)value).reindexArrays();
            }
            list.set(index++, value);
        }
        return list;
    }

    public void fillDelete(Delete delete, String family, FieldPath path) {
        for (Map.Entry<Integer, HValue> entry : elements.entrySet()) {
            HValue value = entry.getValue();
            value.fillDelete(delete, family, value.getFullPath(path, entry.getKey()));
        }
    }

    public void fillPut(Put put, String family, FieldPath path) {
        for (Map.Entry<Integer, HValue> entry : elements.entrySet()) {
            HValue value = entry.getValue();
            value.fillPut(put, family, value.getFullPath(path, entry.getKey()));
        }
    }

    class HListIterator implements ListIterator<Object> {
        ListIterator<HValue> iter;

        public HListIterator(int index) {
            this.iter = Lists.newArrayList(HList.this.elements.values()).listIterator(index);
        }

        public HListIterator() {
            this.iter = Lists.newArrayList(HList.this.elements.values()).listIterator();
        }

        public boolean hasNext() {
            return this.iter.hasNext();
        }

        public Object next() {
            HValue value = this.iter.next();
            return value != null ? value.getObject() : null;
        }

        public boolean hasPrevious() {
            return this.iter.hasPrevious();
        }

        public Object previous() {
            HValue value = this.iter.previous();
            return value != null ? value.getObject() : null;
        }

        public int nextIndex() {
            return this.iter.nextIndex();
        }

        public int previousIndex() {
            return this.iter.previousIndex();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public void set(Object e) {
            throw new UnsupportedOperationException();
        }

        public void add(Object e) {
            throw new UnsupportedOperationException();
        }
    }
}
