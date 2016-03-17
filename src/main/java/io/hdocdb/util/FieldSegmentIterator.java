package io.hdocdb.util;

import org.ojai.FieldSegment;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class FieldSegmentIterator implements Iterator<FieldSegment> {
    FieldSegment current;

    public FieldSegmentIterator(FieldSegment rootSegment) {
        current = rootSegment;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldSegment next() {
        if (current == null) {
            throw new NoSuchElementException();
        }
        FieldSegment ret = current;
        current = current.getChild();
        return ret;
    }

    @Override
    public boolean hasNext() {
        return current != null;
    }
}
