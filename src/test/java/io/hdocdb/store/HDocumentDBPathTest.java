package io.hdocdb.store;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;
import org.ojai.types.ODate;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HDocumentDBPathTest extends HDocumentDBTest {

    @BeforeClass
    public static void setup() throws IOException {
        HDocumentDBTest.setup();
    }

    @AfterClass
    public static void teardown() throws IOException {
        HDocumentDBTest.teardown();
    }

    @Test
    public void testArrayPaths() throws IOException {

        Set<Document> documents = Sets.newHashSet();

        Document document = new HDocument()
                .set("_id", "jdoe")
                .set("first_name", "John")
                .set("last_name", "Doe")
                .set("dob", ODate.parse("1970-06-23"))
                .setArray("array1", new int[] { 1, 2, 3 })
                .setArray("array2", new Map[] {
                        ImmutableMap.of("a", 1, "b", 2 ),
                        ImmutableMap.of("a", 11, "b", 12 ),
                        ImmutableMap.of("a", 21, "b", 22 ) })
                .setArray("array3", new int[][] {
                        { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } })
                .setArray("array4", new Map[][] {
                        { ImmutableMap.of("a", 1, "b", 2 ),
                                ImmutableMap.of("a", 11, "b", 12 ),
                                ImmutableMap.of("a", 21, "b", 22 ) },
                        { ImmutableMap.of("a", 31, "b", 32 ),
                                ImmutableMap.of("a", 41, "b", 42 ),
                                ImmutableMap.of("a", 51, "b", 52 ) },
                        { ImmutableMap.of("a", 61, "b", 62 ),
                                ImmutableMap.of("a", 71, "b", 72 ),
                                ImmutableMap.of("a", 81, "b", 82 ) } })
                .setArray("array5", new int[][][] {
                        { { 1, 2, 3 } }, { { 4 }, { 5, 6 } }, { { 7 }, { 8 }, { 9 } } })
                .setArray("array6", new Map[][][] {
                        { { ImmutableMap.of("a", 1, "b", 2 ),
                                ImmutableMap.of("a", 11, "b", 12 ),
                                ImmutableMap.of("a", 21, "b", 22 ) } },
                        { { ImmutableMap.of("a", 31, "b", 32 ) },
                                { ImmutableMap.of("a", 41, "b", 42 ),
                                ImmutableMap.of("a", 51, "b", 52 ) } },
                        { { ImmutableMap.of("a", 61, "b", 62 ) },
                                { ImmutableMap.of("a", 71, "b", 72 ) },
                                { ImmutableMap.of("a", 81, "b", 82 ) } } });

        assertTrue(document.getList("array1").contains(1));
        assertTrue(document.getList("array2[].a").contains(11));
        assertTrue(document.getList("array3[][]").contains(9));
        assertTrue(document.getList("array4[][].b").contains(82));
        assertTrue(document.getList("array5[][][]").contains(1));
        assertTrue(document.getList("array6[][][].a").contains(61));

        // save document into the table
        mainColl.insertOrReplace(document);
        documents.add(document);
        printDocument("jdoe");

        document = new HDocument()
                .set("_id", "jsmith")
                .set("first_name", "John")
                .set("last_name", "Smith")
                .set("dob", ODate.parse("1972-08-24"))
                .setArray("array1", new int[] { 4, 5, 6 })
                .setArray("array2", new Map[] {
                        ImmutableMap.of("a", 3, "b", 4 ),
                        ImmutableMap.of("a", 13, "b", 14 ),
                        ImmutableMap.of("a", 23, "b", 24 ) })
                .setArray("array3", new int[][] {
                        { 11, 22, 33 }, { 44, 55, 66 }, { 77, 88, 99 } })
                .setArray("array4", new Map[][] {
                        { ImmutableMap.of("a", 4, "b", 5 ),
                                ImmutableMap.of("a", 14, "b", 15 ),
                                ImmutableMap.of("a", 24, "b", 25 ) },
                        { ImmutableMap.of("a", 34, "b", 35 ),
                                ImmutableMap.of("a", 44, "b", 45 ),
                                ImmutableMap.of("a", 54, "b", 55 ) },
                        { ImmutableMap.of("a", 64, "b", 62 ),
                                ImmutableMap.of("a", 74, "b", 75 ),
                                ImmutableMap.of("a", 84, "b", 85 ) } })
                .setArray("array5", new int[][][] {
                        { { 11, 22, 33 } }, { { 44 }, { 55, 66 } }, { { 77 }, { 88 }, { 99 } } })
                .setArray("array6", new Map[][][] {
                        { { ImmutableMap.of("a", 4, "b", 5 ),
                                ImmutableMap.of("a", 14, "b", 15 ),
                                ImmutableMap.of("a", 24, "b", 25 ) } },
                        { { ImmutableMap.of("a", 34, "b", 35 ) },
                                { ImmutableMap.of("a", 44, "b", 45 ),
                                        ImmutableMap.of("a", 54, "b", 55 ) } },
                        { { ImmutableMap.of("a", 64, "b", 65 ) },
                                { ImmutableMap.of("a", 74, "b", 75 ) },
                                { ImmutableMap.of("a", 84, "b", 85 ) } } });

        // save document into the table
        mainColl.insertOrReplace(document);
        documents.add(document);
        printDocument("jsmith");

        mainColl.flush(); // flush to the server

        int cnt = 0;
        try (DocumentStream documentStream = mainColl.find(new HQueryCondition().is("array1[]", QueryCondition.Op.EQUAL, 1))) {
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
                assertTrue(doc.getList("array1").contains(1));
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(new HQueryCondition().is("array2[].a", QueryCondition.Op.EQUAL, 3))) {
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
                assertTrue(doc.getList("array2[].a").contains(3));
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(new HQueryCondition().is("array3[][]", QueryCondition.Op.EQUAL, 9))) {
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
                assertTrue(doc.getList("array3[][]").contains(9));
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(new HQueryCondition().is("array4[][].b", QueryCondition.Op.EQUAL, 85))) {
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
                assertTrue(doc.getList("array4[][].b").contains(85));
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(new HQueryCondition().is("array5[][][]", QueryCondition.Op.EQUAL, 1))) {
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
                assertTrue(doc.getList("array5[][][]").contains(1));
            }
        }
        assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(new HQueryCondition().is("array6[][][].a", QueryCondition.Op.EQUAL, 4))) {
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
                assertTrue(doc.getList("array6[][][].a").contains(4));
            }
        }
        assertEquals(1, cnt);
    }


    private static void printDocument(String id) {
        // get a single document
        Document record = mainColl.findById(new HValue(id));
        System.out.print("Single record\n\t");
        System.out.println(record);

        //print individual fields
        System.out.println("Id : " + record.getIdString() + " - first name : " + record.getString("first_name"));
    }

}
