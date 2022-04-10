package io.hdocdb.store;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.hdocdb.HDocument;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.store.QueryCondition;

import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDocumentFilter extends FilterBase implements Externalizable {

    private static final Pattern ARRAY_WILDCARD = Pattern.compile("\\[\\]");

    private ConditionNode condition;
    private String[] paths;

    public HDocumentFilter() {
        this(null, (String[])null);
    }

    public HDocumentFilter(QueryCondition c, String... paths) {
        this.condition = c != null && ((HQueryCondition)c).getRoot() != null
                ? ((HQueryCondition)c).getRoot()
                : new ConditionLeaf();
        this.paths = paths != null ? paths : new String[0];
    }

    @Override
    public Filter.ReturnCode filterCell(Cell cell) {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
    }

    @Override
    public boolean hasFilterRow() {
        return true;
    }

    @Override
    public void filterRowCells(List<Cell> kvs) {
        List<String> fieldPaths = Arrays.asList(paths);
        HDocument doc = new HDocument(kvs);
        if (condition.evaluate(doc)) {
            if (!fieldPaths.isEmpty()) {
                Iterables.removeIf(kvs, new Predicate<>() {
                    @Override
                    public boolean apply(@Nullable Cell cell) {
                        String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                        if (columnName.equals(HDocument.TS)) return false;
                        return !matchesPaths(fieldPaths, columnName);
                    }
                });
            }
        } else {
            kvs.clear();
        }
    }

    private static boolean matchesPaths(List<String> queryPaths, String path) {
        for (String p : queryPaths) {
            if (matchesPath(p, path)) return true;
        }
        return false;
    }

    private static boolean matchesPath(String queryPath, String path) {
        if (queryPath.contains("[]")) {
            Matcher m = ARRAY_WILDCARD.matcher(queryPath);
            StringBuilder sb = new StringBuilder("^");
            int lastAppendPosition = 0;
            while (m.find()) {
                String substr = queryPath.substring(lastAppendPosition, m.start());
                sb.append(Pattern.quote(substr));
                sb.append("\\[\\d+\\]");
                lastAppendPosition = m.end();
            }
            sb.append(queryPath, lastAppendPosition, queryPath.length());
            String queryRegex = sb.toString();
            return path.matches(queryRegex);
        } else {
            return path.startsWith(queryPath);
        }
    }

    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException {
        boolean isLeaf = input.readByte() == 1;
        ConditionNode node = isLeaf ? new ConditionLeaf() : new ConditionParent();
        node.readExternal(input);
        int numPaths = input.readInt();
        String[] paths = new String[numPaths];
        for (int i = 0; i < numPaths; i++) {
            paths[i] = input.readUTF();
        }
        this.condition = node;
        this.paths = paths;
    }

    public void writeExternal(ObjectOutput output) throws IOException {
        output.writeByte(condition.isLeaf() ? 1 : 0);
        condition.writeExternal(output);
        output.writeInt(paths.length);
        for (String path : paths) {
            output.writeUTF(path);
        }
    }

    @Override
    public byte[] toByteArray() throws IOException {
        Codec<HDocumentFilter> codec = new Codec<>();
        return codec.encode(this);
    }

    public static Filter parseFrom(byte[] pbBytes) throws DeserializationException {
        try {
            Codec<HDocumentFilter> codec = new Codec<>();
            HDocumentFilter filter = codec.decode(pbBytes, new HDocumentFilter());
            return filter;
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }
}
