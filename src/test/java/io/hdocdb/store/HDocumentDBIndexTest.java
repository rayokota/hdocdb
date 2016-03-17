package io.hdocdb.store;

import com.google.common.collect.ImmutableMap;
import io.hdocdb.HDocument;
import io.hdocdb.HDocumentStream;
import io.hdocdb.HValue;
import io.hdocdb.compile.QueryIndexCompiler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.Value;
import org.ojai.store.DocumentMutation;
import org.ojai.store.QueryCondition;
import org.ojai.types.OTimestamp;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HDocumentDBIndexTest extends HDocumentDBTest {

    @BeforeClass
    public static void setup() throws IOException {
        HDocumentDBTest.setup();
    }

    @AfterClass
    public static void teardown() throws IOException {
        HDocumentDBTest.teardown();
    }

    @Test
    public void testIndex1() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("testindex", "z.a", Value.Type.INT, Order.ASCENDING, false);

        Document newDoc = new HDocument();
        newDoc.set("name", "foo");
        newDoc.set("z", ImmutableMap.of("a", 17, "b", 4));
        newDoc.setId("d1");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("z.a", QueryCondition.Op.EQUAL, 17))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(17, doc.getMap("z").get("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        newDoc = new HDocument();
        newDoc.set("name", "bar");
        newDoc.set("z", ImmutableMap.of("a", 18));
        newDoc.setId("d2");
        coll.insert(newDoc);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("z.a", QueryCondition.Op.GREATER_OR_EQUAL, 17))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndex2() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("a", "a", Value.Type.INT, Order.ASCENDING, false);
        coll.createIndex("b", "b", Value.Type.INT, Order.ASCENDING, false);
        coll.createIndex("c", "c", Value.Type.INT, Order.ASCENDING, false);

        Document newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.set("b", 1);
        newDoc.setId("d1");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.setId("d2");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.setId("d3");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.setId("d4");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 2))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(2, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(4, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("b", new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 2))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "b");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(2, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(4, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("c", new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 2))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "c");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(2, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(4, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexBounds() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("a", "a", Value.Type.INT, Order.ASCENDING, false);

        Document newDoc = new HDocument();
        newDoc.set("a", Integer.MIN_VALUE);
        newDoc.setId("d1");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", 1);
        newDoc.setId("d2");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", Integer.MAX_VALUE);
        newDoc.setId("d3");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.LESS_OR_EQUAL, Integer.MIN_VALUE))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(Integer.MIN_VALUE, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.LESS_OR_EQUAL, 1))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.LESS_OR_EQUAL, Integer.MAX_VALUE))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(3, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.LESS, Integer.MIN_VALUE))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(0, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.LESS, 1))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(Integer.MIN_VALUE, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.LESS, Integer.MAX_VALUE))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.GREATER, Integer.MAX_VALUE))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(0, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.GREATER, 1))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(Integer.MAX_VALUE, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.GREATER, Integer.MIN_VALUE))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.GREATER_OR_EQUAL, Integer.MAX_VALUE))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(Integer.MAX_VALUE, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.GREATER_OR_EQUAL, 1))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.GREATER_OR_EQUAL, Integer.MIN_VALUE))) {

            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(3, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexUpdate() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("a", "a", Value.Type.INT, Order.ASCENDING, false);

        Document newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.setId("d1");
        coll.insert(newDoc);
        DocumentMutation mutation = new HDocumentMutation();
        mutation.setOrReplace("a", 3);
        coll.update(new HValue("d1"), mutation);

        int cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.LESS, 4))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(3, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
            // index is not marked stale because it led to a valid document (even though it had a different value)
            assertEquals(((HDocumentStream)documentStream).explain().getStaleIndexesRunningCount(), 0);
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.findWithIndex("a", new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 2))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "a");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
            assertEquals(((HDocumentStream)documentStream).explain().getStaleIndexesRunningCount(), 1);
        }
        assertEquals(0, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexMid() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("ts", "ts", Value.Type.TIMESTAMP, Order.ASCENDING, false);

        OTimestamp mid = null;
        for (int i = 1; i < 100; i++) {
            OTimestamp ts = new OTimestamp(System.currentTimeMillis() + i);
            Document newDoc = new HDocument();
            newDoc.set("a", i);
            newDoc.set("ts", ts);
            newDoc.setArray("cats", new int[] { i, i+1, i+2 });
            if (i == 51) {
                mid = ts;
            }
            newDoc.setId("d" + i);
            coll.insert(newDoc);
        }


        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("ts", QueryCondition.Op.LESS, mid))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "ts");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(50, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexNull() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("x", "x", Value.Type.INT, Order.ASCENDING, false);

        Document newDoc = new HDocument();
        newDoc.set("x", 2);
        newDoc.setId("d1");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("y", 3);
        newDoc.setId("d2");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("x", 4);
        newDoc.setId("d3");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("x", QueryCondition.Op.EQUAL, 2))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "x");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(2, doc.getInt("x"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().notExists("x"))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), null);
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(3, doc.getInt("y"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("x", QueryCondition.Op.EQUAL, 4))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "x");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(4, doc.getInt("x"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexMultipleFields() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.newIndexBuilder("ab")
                .add("a", Value.Type.INT, Order.ASCENDING)
                .add("b", Value.Type.INT, Order.ASCENDING)
                .setAsync(false)
                .build();

        Document newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.set("b", 3);
        newDoc.setId("d1");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.set("b", 3);
        newDoc.setId("d2");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.set("b", 4);
        newDoc.setId("d3");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", 1);
        newDoc.set("b", 3);
        newDoc.setId("d4");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 2))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "ab");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(2, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(3, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().and().is("a", QueryCondition.Op.EQUAL, 2)
                .is("b", QueryCondition.Op.EQUAL, 3).close())) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "ab");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(2, doc.getInt("a"));
                assertEquals(3, doc.getInt("b"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexMultipleFields2() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.newIndexBuilder("ab")
                .add("a.b", Value.Type.INT, Order.ASCENDING)
                .add("a.c", Value.Type.INT, Order.ASCENDING)
                .setAsync(false)
                .build();

        Document newDoc = new HDocument();
        newDoc.set("a", ImmutableMap.of("b", 3, "c", 3));
        newDoc.setId("d1");
        coll.insert(newDoc);
        newDoc = new HDocument();
        newDoc.set("a", ImmutableMap.of("b", 1, "c", 5));
        newDoc.setId("d2");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().and()
                .is("a.b", QueryCondition.Op.GREATER, 2)
                .is("a.c", QueryCondition.Op.LESS, 4).close())) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "ab");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(3, doc.getInt("a.b"));
                assertEquals(3, doc.getInt("a.c"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexMapAsNull() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("testindex", "z", Value.Type.INT, Order.ASCENDING, false);

        Document newDoc = new HDocument();
        newDoc.set("z", ImmutableMap.of("a", 17, "b", 4));
        newDoc.setId("d1");
        coll.insert(newDoc);

        newDoc = new HDocument();
        newDoc.set("z", 2);
        newDoc.setId("d2");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("z", QueryCondition.Op.EQUAL, 2))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(2, doc.getInt("z"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().exists("z"))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), null);
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexWithProjection() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        Document newDoc = new HDocument();
        newDoc.set("a", 1);
        newDoc.set("b", 2);
        newDoc.setId("d1");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 1), "b")) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), null);
            for (Document doc : documentStream) {
                cnt++;
                assertNull(doc.getValue("a"));
                assertEquals(2, doc.getInt("b"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        coll.createIndex("testindex", "a", Value.Type.INT, Order.ASCENDING, false);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 1), "b")) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                assertNull(doc.getValue("a"));
                assertEquals(2, doc.getInt("b"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexPopulate() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        Document newDoc = new HDocument();
        newDoc.set("name", "foo");
        newDoc.set("z", ImmutableMap.of("a", 17, "b", 4));
        newDoc.setId("d1");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("z.a", QueryCondition.Op.EQUAL, 17))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), null);
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(17, doc.getMap("z").get("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        coll.createIndex("testindex", "z.a", Value.Type.INT, Order.ASCENDING, false);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("z.a", QueryCondition.Op.EQUAL, 17))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(17, doc.getMap("z").get("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexPopulate2() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        for (int i = 0; i < 100; i++) {
            Document newDoc = new HDocument();
            newDoc.set("a", i);
            newDoc.setId("d" + i);
            coll.insert(newDoc);
        }

        int cnt = 0;
        long start = System.currentTimeMillis();
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 99))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), null);
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(99, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        System.out.println("Time lapsed: " + (System.currentTimeMillis() - start));
        assertEquals(1, cnt);

        coll.createIndex("testindex", "a", Value.Type.INT, Order.ASCENDING, false);

        cnt = 0;
        start = System.currentTimeMillis();
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("a", QueryCondition.Op.EQUAL, 99))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(99, doc.getInt("a"));
                //System.out.println("\t" + doc);
            }
        }
        System.out.println("Time lapsed: " + (System.currentTimeMillis() - start));
        assertEquals(1, cnt);

        closeDocumentCollection(coll);
    }

    @Test
    public void testIndexDrop() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        coll.createIndex("testindex", "z.a", Value.Type.INT, Order.ASCENDING, false);
        coll.createIndex("testindex2", "z.a", Value.Type.INT, Order.ASCENDING, false);

        Document newDoc = new HDocument();
        newDoc.set("name", "foo");
        newDoc.set("z", ImmutableMap.of("a", 17, "b", 4));
        newDoc.setId("d1");
        coll.insert(newDoc);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("z.a", QueryCondition.Op.EQUAL, 17))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(17, doc.getMap("z").get("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        assertEquals(1, coll.getIndexSize("testindex"));
        assertEquals(1, coll.getIndexSize("testindex2"));

        coll.dropIndex("testindex", false);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("z.a", QueryCondition.Op.EQUAL, 17))) {
            assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex2");
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(17, doc.getMap("z").get("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        assertEquals(0, coll.getIndexSize("testindex"));
        assertEquals(1, coll.getIndexSize("testindex2"));

        closeDocumentCollection(coll);
    }

}
