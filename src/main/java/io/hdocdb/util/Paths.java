package io.hdocdb.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.ojai.FieldPath;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Paths {

    public static String[] asPathStrings(FieldPath... fields) {
        String[] paths = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            paths[i] = fields[i].asPathString();
        }
        return paths;
    }

    public static String[] asPathStrings(Collection<FieldPath> fieldPaths, String... paths) {
        Set<String> allPaths = Sets.newHashSet();
        if (fieldPaths != null) {
            for (FieldPath fieldPath : fieldPaths) {
                allPaths.add(fieldPath.asPathString());
            }
        }
        if (paths != null) {
            for (String path : paths) {
                allPaths.add(path);
            }
        }
        List<String> result = Lists.newArrayList(allPaths);
        return result.toArray(new String[result.size()]);
    }

    public static String[] asPathStrings(Collection<FieldPath> fieldPaths1, Collection<FieldPath> fieldPaths2) {
        Set<String> allPaths = Sets.newHashSet();
        if (fieldPaths1 != null) {
            for (FieldPath fieldSegments : fieldPaths1) {
                allPaths.add(fieldSegments.asPathString());
            }
        }
        if (fieldPaths2 != null) {
            for (FieldPath fieldSegments : fieldPaths2) {
                allPaths.add(fieldSegments.asPathString());
            }
        }
        List<String> result = Lists.newArrayList(allPaths);
        return result.toArray(new String[result.size()]);
    }

    public static String[] asPathStrings(org.graalvm.polyglot.Value json) {
        if (json == null) return new String[0];
        List<String> paths = Lists.newArrayList();
        for (String key : json.getMemberKeys()) {
            org.graalvm.polyglot.Value value = json.getMember(key);
            if (value.isNumber()) {
                if (value.asInt() != 0) paths.add(key);
            }
        }
        return paths.toArray(new String[paths.size()]);
    }
}
