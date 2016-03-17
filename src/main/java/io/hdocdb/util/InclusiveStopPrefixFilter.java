package io.hdocdb.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A Filter that stops after a row that is greater than the given prefix.
 * <p/>
 * Use this filter to include the stop row, eg: [A,Z].
 */
public class InclusiveStopPrefixFilter extends FilterBase implements Externalizable {
    private byte[] stopRowPrefix;
    private boolean done = false;

    public InclusiveStopPrefixFilter() {
    }

    public InclusiveStopPrefixFilter(final byte[] stopRowPrefix) {
        this.stopRowPrefix = stopRowPrefix;
    }

    public byte[] getStopRowPrefix() {
        return this.stopRowPrefix;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (done) return ReturnCode.NEXT_ROW;
        return ReturnCode.INCLUDE;
    }

    // Override here explicitly as the method in super class FilterBase might do a KeyValue recreate.
    // See HBASE-12068
    @Override
    public Cell transformCell(Cell v) {
        return v;
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        if (buffer == null) {
            //noinspection RedundantIfStatement
            if (this.stopRowPrefix == null) {
                return true; //filter...
            }
            return false;
        }
        // if stopRowPrefix is <= buffer, then true, filter row.
        // the only difference between InclusiveStopFilter is to use stopRowPrefix.length instead of length below
        int cmp = Bytes.compareTo(stopRowPrefix, 0, stopRowPrefix.length,
                buffer, offset, stopRowPrefix.length);

        if (cmp < 0) {
            done = true;
        }
        return done;
    }

    public boolean filterAllRemaining() {
        return done;
    }

    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException {
        int len = input.readInt();
        byte[] stopRowPrefix = new byte[len];
        if (len > 0) input.readFully(stopRowPrefix, 0, len);
        this.stopRowPrefix = stopRowPrefix;
    }

    public void writeExternal(ObjectOutput output) throws IOException {
        output.writeInt(stopRowPrefix.length);
        output.write(stopRowPrefix);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() throws IOException {
        Codec<InclusiveStopPrefixFilter> codec = new Codec<>();
        return codec.encode(this);
    }

    /**
     * @param pbBytes A pb serialized {@link InclusiveStopPrefixFilter} instance
     * @return An instance of {@link InclusiveStopPrefixFilter} made from <code>bytes</code>
     * @throws DeserializationException
     * @see #toByteArray
     */
    @SuppressWarnings("unchecked")
    public static InclusiveStopPrefixFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        try {
            Codec<InclusiveStopPrefixFilter> codec = new Codec<>();
            InclusiveStopPrefixFilter filter = codec.decode(pbBytes, new InclusiveStopPrefixFilter());
            return filter;
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " " + Bytes.toStringBinary(this.stopRowPrefix);
    }
}
