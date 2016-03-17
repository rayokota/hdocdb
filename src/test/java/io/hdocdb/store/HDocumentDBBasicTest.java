package io.hdocdb.store;

import com.google.common.collect.ImmutableList;
import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentMutation;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class HDocumentDBBasicTest extends HDocumentDBTest {

    @BeforeClass
    public static void setup() throws IOException {
        HDocumentDBTest.setup();
    }

    @AfterClass
    public static void teardown() throws IOException {
        HDocumentDBTest.teardown();
    }

    @Test
    public void testBasic1() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        Document newDoc = new HDocument();
        newDoc.set("a", 1);
        newDoc.setId("d1");
        coll.insert(newDoc);

        Document doc = coll.findById(new HValue("d1"));
        assertEquals(1, doc.getInt("a"));

        newDoc.set("a", 2);
        coll.replace(newDoc);

        doc = coll.findById(new HValue("d1"));
        assertEquals(2, doc.getInt("a"));


        closeDocumentCollection(coll);
    }

    @Test
    public void testBasic2() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        Document newDoc = new HDocument();
        newDoc.set("n", 2);
        newDoc.setId("d1");
        coll.insert(newDoc);

        Document doc = coll.findById(new HValue("d1"));
        assertEquals(2, doc.getInt("n"));

        coll.delete(newDoc);

        doc = coll.findById(new HValue("d1"));
        assertEquals(null, doc);

        closeDocumentCollection(coll);
    }

    @Test
    public void testBasic5() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        Document newDoc = new HDocument();
        newDoc.set("a", 2);
        newDoc.setArray("b", new int[] { 1, 2, 3 } );
        newDoc.setId("d1");
        coll.insert(newDoc);

        Document doc = coll.findById(new HValue("d1"));
        assertEquals(3, doc.getList("b").size());

        closeDocumentCollection(coll);
    }

    @Test
    public void testUpdate2() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        Document newDoc = new HDocument();
        newDoc.set("a", 4);
        newDoc.setId("d1");
        coll.insert(newDoc);

        Document doc = coll.findById(new HValue("d1"));
        assertEquals(4, doc.getInt("a"));

        DocumentMutation mutation = new HDocumentMutation();
        mutation.increment("a", 2);
        coll.update(new HValue("d1"), mutation);

        doc = coll.findById(new HValue("d1"));
        assertEquals(6, doc.getInt("a"));

        closeDocumentCollection(coll);
    }


    @Test
    public void testArrayInsert() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();

        Document newDoc = new HDocument();
        newDoc.setArray("a", new int[] { 1, 2 });
        newDoc.setId("d1");
        coll.insert(newDoc);

        DocumentMutation mutation = new HDocumentMutation();
        mutation.set("a[0]", 5);
        coll.update(new HValue("d1"), mutation);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find()) {
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(ImmutableList.of(5, 2), doc.getList("a"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        closeDocumentCollection(coll);
    }
}
