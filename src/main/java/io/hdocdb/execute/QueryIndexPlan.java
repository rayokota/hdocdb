package io.hdocdb.execute;

import com.google.common.collect.Maps;
import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import io.hdocdb.store.*;
import io.hdocdb.util.Codec;
import io.hdocdb.util.InclusiveStopPrefixFilter;
import io.hdocdb.util.Paths;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class QueryIndexPlan {

    private static final Logger LOG = LoggerFactory.getLogger(QueryIndexPlan.class);

    private Table indexTable;
    private Scan scan;
    private Index index;
    private List<ConditionRange> ranges;
    private QueryCondition condition;
    private String[] paths;

    public QueryIndexPlan(Table indexTable, Scan scan, Index index, List<ConditionRange> ranges,
                          QueryCondition c, String... paths) {
        this.indexTable = indexTable;
        this.scan = scan;
        this.index = index;
        this.ranges = ranges;
        this.condition = c;
        this.paths = paths;
    }

    public Index getIndex() {
        return index;
    }

    public List<ConditionRange> getRanges() {
        return ranges;
    }

    public int size() {
        return ranges.size();
    }

    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryIndexPlan indexPlan = (QueryIndexPlan) o;

        if (!index.equals(indexPlan.index)) return false;
        return ranges.equals(indexPlan.ranges);

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + ranges.hashCode();
        return result;
    }

    public IndexQueries execute() {
        try {
            if (ranges.size() > 0) {
                setupRangeScan();
            } else {
                setupPrefixScan(true);
            }

            Codec<HValueHolder> codec = new Codec<>();
            // a list of point gets
            IndexQueries indexQueries = new IndexQueries(indexTable, index, ranges);
            // a list of docs
            Map<HValue, HDocument> docs = Maps.newHashMap();
            try (ResultScanner indexResult = indexTable.getScanner(scan)) {
                for (Result result : indexResult) {
                    byte[] indexRowKey = result.getRow();

                    boolean skip = false;
                    PositionedByteRange putKey = new SimplePositionedMutableByteRange(indexRowKey);
                    OrderedBytes.decodeString(putKey);

                    HDocument doc = new HDocument();
                    for (IndexFieldPath element : index.getFields()) {
                        String path = OrderedBytes.decodeString(putKey);
                        if (!path.equals(element.getPath().asPathString())) {
                            LOG.warn("Found mismatching path: " + path + ", " + element.getPath().asPathString());
                            skip = true;
                            break;
                        }
                        Value.Type type = element.getType();
                        HValue v = new HValue();
                        if (OrderedBytes.isNull(putKey)) {
                            v.orderedDecode(putKey, Value.Type.NULL);
                        } else {
                            v.orderedDecode(putKey, type);
                            doc.setHValue(element.getPath(), v);
                        }
                    }
                    if (skip) continue;

                    // decode id
                    OrderedBytes.decodeString(putKey);
                    byte[] idBytes = OrderedBytes.decodeBlobCopy(putKey);
                    HValue id = codec.decode(idBytes, new HValueHolder()).getValue();
                    doc.setId(id);

                    Cell indexTsCell = result.getColumnLatestCell(Bytes.toBytes(Index.DEFAULT_FAMILY), Bytes.toBytes(HDocument.TS));
                    HValue indexTsValue = codec.decode(CellUtil.cloneValue(indexTsCell), new HValueHolder()).getValue();
                    long indexTs = indexTsValue.getTimestampAsLong();

                    if (indexQueries.getConditionFromRanges().evaluate(doc)) {
                        if (docs.containsKey(id)) continue;
                        docs.put(id, doc);
                        Get get = new Get(idBytes);
                        String[] allPaths = condition != null
                            ? Paths.asPathStrings(((HQueryCondition)condition).getConditionPaths(), paths)
                            : paths;
                        if (allPaths != null && allPaths.length > 0) {
                            // don't set a condition on the filter for the Get as we want to check
                            // the condition on the client to determine if the index is stale
                            get.setFilter(new HDocumentFilter(null, allPaths));
                        }
                        indexQueries.getQueries().add(new IndexQuery(ByteBuffer.wrap(indexRowKey), indexTs, get));
                    }
                }
            }
            return indexQueries;
        } catch (ClassNotFoundException | IOException e) {
            throw new StoreException(e);
        }
    }

    private void setupRangeScan() throws IOException {
        PositionedByteRange startRow = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(startRow, index.getName(), org.apache.hadoop.hbase.util.Order.ASCENDING);
        PositionedByteRange stopRow = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(stopRow, index.getName(), org.apache.hadoop.hbase.util.Order.ASCENDING);

        for (int i = 0; i < ranges.size(); i++) {
            ConditionRange range = ranges.get(i);
            IndexFieldPath element = index.getField(i);
            FieldPath path = range.getField();
            if (range.isSingleton()) {
                OrderedBytes.encodeString(startRow, path.asPathString(), org.apache.hadoop.hbase.util.Order.ASCENDING);
                range.getRange().lowerEndpoint().orderedEncode(startRow, element.getOrder());
                OrderedBytes.encodeString(stopRow, path.asPathString(), org.apache.hadoop.hbase.util.Order.ASCENDING);
                range.getRange().upperEndpoint().orderedEncode(stopRow, element.getOrder());
            } else {
                if (element.getOrder() == Order.ASCENDING) {
                    if (range.getRange().hasLowerBound()) {
                        OrderedBytes.encodeString(startRow, path.asPathString(), org.apache.hadoop.hbase.util.Order.ASCENDING);
                        range.getRange().lowerEndpoint().orderedEncode(startRow, element.getOrder());
                    }
                    if (range.getRange().hasUpperBound()) {
                        OrderedBytes.encodeString(stopRow, path.asPathString(), org.apache.hadoop.hbase.util.Order.ASCENDING);
                        range.getRange().upperEndpoint().orderedEncode(stopRow, element.getOrder());
                    }
                } else {
                    if (range.getRange().hasUpperBound()) {
                        OrderedBytes.encodeString(startRow, path.asPathString(), org.apache.hadoop.hbase.util.Order.ASCENDING);
                        range.getRange().upperEndpoint().orderedEncode(startRow, element.getOrder());
                    }
                    if (range.getRange().hasLowerBound()) {
                        OrderedBytes.encodeString(stopRow, path.asPathString(), org.apache.hadoop.hbase.util.Order.ASCENDING);
                        range.getRange().lowerEndpoint().orderedEncode(stopRow, element.getOrder());
                    }
                }
                // only process one non-singleton after all singletons
                break;
            }
        }
        startRow.setLength(startRow.getPosition());
        startRow.setPosition(0);
        byte[] startRowBytes = new byte[startRow.getRemaining()];
        startRow.get(startRowBytes);
        stopRow.setLength(stopRow.getPosition());
        stopRow.setPosition(0);
        byte[] stopRowBytes = new byte[stopRow.getRemaining()];
        stopRow.get(stopRowBytes);
        scan.setStartRow(startRowBytes);
        if (stopRowBytes.length > 0) {
            scan.setFilter(new InclusiveStopPrefixFilter(stopRowBytes));
        } else {
            setupPrefixScan(false);
        }
    }

    private void setupPrefixScan(boolean setStartRow) {
        PositionedByteRange startRow = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(startRow, index.getName(), org.apache.hadoop.hbase.util.Order.ASCENDING);
        startRow.setLength(startRow.getPosition());
        startRow.setPosition(0);
        byte[] startRowBytes = new byte[startRow.getRemaining()];
        startRow.get(startRowBytes);

        if (setStartRow) scan.setStartRow(startRowBytes);
        scan.setFilter(new PrefixFilter(startRowBytes));
    }

}
