package io.hdocdb.store;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.hdocdb.HDocument;
import io.hdocdb.HDocumentStream;
import io.hdocdb.HValue;
import io.hdocdb.store.model.User;
import io.hdocdb.store.model.User2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.json.Json;
import org.ojai.store.DocumentMutation;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.DocumentExistsException;
import org.ojai.types.ODate;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

public class HDocumentDBCRUDTest extends HDocumentDBTest {

    @BeforeClass
    public static void setup() throws IOException {
        HDocumentDBTest.setup();
        createDocuments();
    }

    @AfterClass
    public static void teardown() throws IOException {
        HDocumentDBTest.teardown();
    }

    private static void createDocuments() throws IOException {

        Set<Document> documents = Sets.newHashSet();

        // Create a new document (simple format)
        Document document = new HDocument()
                .set("_id", "jdoe")
                .set("first_name", "John")
                .set("last_name", "Doe")
                .set("dob", ODate.parse("1970-06-23"));

        // save document into the table
        mainColl.insertOrReplace(document);
        documents.add(document);
        printDocument("jdoe");


        // create a new document without _id
        document = new HDocument()
                .set("first_name", "David")
                .set("last_name", "Simon")
                .set("dob", ODate.parse("1980-10-13"))
        ;
        document.setId(new HValue("dsimon"));

        mainColl.insert(new HValue("dsimon"), document);
        documents.add(((HDocument)document).shallowCopy().setId(new HValue("dsimon")));
        printDocument("dsimon");


        // create a new document from a simple bean
        // look at the User class to see how you can use JSON Annotation to drive the format of the document
        User user = new User();
        user.setId("alehmann");
        user.setFirstName("Andrew");
        user.setLastName("Lehmann");
        user.setDob(ODate.parse("1980-10-13"));
        user.addInterest("html");
        user.addInterest("css");
        user.addInterest("js");
        document = Json.newDocument(user);

        // save document into the table
        mainColl.insertOrReplace(document);
        documents.add(HValue.initFromDocument(document));
        printDocument("alehmann");


        // try to insert the same document ID
        try {
            mainColl.insert(new HValue("dsimon"), document);
        } catch (DocumentExistsException dee) {
            System.out.println("Exception during insert : " + dee.getMessage());
        }


        // Create more complex Record
        document = new HDocument()
                .set("_id", "mdupont")
                .set("first_name", "Maxime")
                .set("last_name", "Dupont")
                .set("dob", ODate.parse("1982-02-03"))
                .set("interests", Arrays.asList("sports", "movies", "electronics"))
                .set("address.line", "1223 Broadway")
                .set("address.city", "San Jose")
                .set("address.zip", 95109)
        ;
        mainColl.insert(document);
        documents.add(document);
        printDocument("mdupont");


        // Another way to create sub document
        // Create the sub document as document and use it to set the value
        Document addressRecord = new HDocument()
                .set("line", "100 Main Street")
                .set("city", "San Francisco")
                .set("zip", 94105);

        document = new HDocument()
                .set("_id", "rsmith")
                .set("first_name", "Robert")
                .set("last_name", "Smith")
                .set("dob", ODate.parse("1982-02-03"))
                .set("interests", Arrays.asList("electronics", "music", "sports"))
                .set("address", addressRecord)
        ;
        mainColl.insert(document);
        documents.add(document);
        printDocument("rsmith");


        mainColl.flush(); // flush to the server

    }


    private static void printDocument(String id) {
        // get a single document
        Document record = mainColl.findById(new HValue(id));
        System.out.print("Single record\n\t");
        System.out.println(record);

        //print individual fields
        System.out.println("Id : " + record.getIdString() + " - first name : " + record.getString("first_name"));
    }


    @Test
    public void queryDocuments() throws Exception {

        // get a single document
        Document record = mainColl.findById(new HValue("mdupont"));
        assertEquals("mdupont", record.getIdString());
        assertEquals("Maxime", record.getString("first_name"));
        assertEquals("Dupont", record.getString("last_name"));

        // get a single document with project
        record = mainColl.findById(new HValue("mdupont"), "last_name");
        assertEquals("mdupont", record.getIdString());
        assertNull(record.getString("first_name"));
        assertEquals("Dupont", record.getString("last_name"));

        // get single document and map it to the bean
        User user = mainColl.findById(new HValue("alehmann")).toJavaBean(User.class);
        assertEquals("alehmann", user.getId());
        assertEquals("Andrew", user.getFirstName());
        assertEquals("Lehmann", user.getLastName());

        // all records in the table
        DocumentStream rs = mainColl.find();
        Iterator<? extends Document> itrs = rs.iterator();
        Document readRecord;
        Set<Document> readRecords = Sets.newHashSet();
        while (itrs.hasNext()) {
            readRecord = itrs.next();
            readRecords.add(readRecord);
            //System.out.println("\t" + readRecord);
        }
        assertEquals(5, readRecords.size());
        rs.close();

        // all records in the table with projection
        int cnt = 0;
        try (DocumentStream documentStream = mainColl.find("first_name", "last_name")) {
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(3, doc.size());
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(5, cnt);

        // all records and use a POJO
        // it is interesting to see how you can ignore unknown attributes with the JSON Annotations
        cnt = 0;
        try (DocumentStream documentStream = mainColl.find()) {
            for (Document doc : documentStream) {
                cnt++;
                User u = doc.toJavaBean(User.class);
                assertTrue(ImmutableList.of("Doe", "Simon", "Smith", "Dupont", "Lehmann").contains(u.getLastName()));
                //System.out.println("\t" + doc.toJavaBean(User.class));
            }
        }
        assertEquals(5, cnt);


        // find with condition
        // Condition equals a string
        QueryCondition condition = new HQueryCondition()
                .is("last_name", QueryCondition.Op.EQUAL, "Doe")
                .build();
        //System.out.println("\n\nCondition: " + condition);
        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(condition)) {
            ((HDocumentStream)documentStream).explain();
            for (Document doc : documentStream) {
                cnt++;
                assertEquals("Doe", doc.getString("last_name"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        // find with condition and projection
        // Condition equals a string
        condition = new HQueryCondition()
                .is("last_name", QueryCondition.Op.EQUAL, "Doe")
                .build();
        //System.out.println("\n\nCondition: " + condition);
        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(condition, "last_name")) {
            for (Document doc : documentStream) {
                cnt++;
                assertEquals("Doe", doc.getString("last_name"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        // Condition as date range
        condition = new HQueryCondition()
                .and()
                .is("dob", QueryCondition.Op.GREATER_OR_EQUAL, ODate.parse("1980-01-01"))
                .is("dob", QueryCondition.Op.LESS, ODate.parse("1981-01-01"))
                .close()
                .build();
        //System.out.println("\n\nCondition: " + condition);
        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(condition)) {
            for (Document doc : documentStream) {
                cnt++;
                assertTrue(doc.getDate("dob").compareTo(ODate.parse("1980-01-01")) > 0);
                assertTrue(doc.getDate("dob").compareTo(ODate.parse("1981-01-01")) < 0);
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        // Condition in sub document
        condition = new HQueryCondition()
                .is("address.zip", QueryCondition.Op.EQUAL, 95109)
                .build();
        //System.out.println("\n\nCondition: " + condition);
        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(condition)) {
            for (Document doc : documentStream) {
                cnt++;
                assertEquals(95109, doc.getInt("address.zip"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

        // Contains a specific value in an array
        condition = new HQueryCondition()
                .is("interests[]", QueryCondition.Op.EQUAL, "sports")
                .build();
        //System.out.println("\n\nCondition: " + condition);
        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(condition)) {
            for (Document doc : documentStream) {
                cnt++;
                assertTrue(doc.getList("interests").contains("sports"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(2, cnt);

        // Contains a value at a specific index
        condition = new HQueryCondition()
                .is("interests[0]", QueryCondition.Op.EQUAL, "sports")
                .build();
        //System.out.println("\n\nCondition: " + condition);
        cnt = 0;
        try (DocumentStream documentStream = mainColl.find(condition, "first_name", "last_name", "interests")) {
            for (Document doc : documentStream) {
                cnt++;
                assertEquals("sports", doc.getString("interests[0]"));
                //System.out.println("\t" + doc);
            }
        }
        assertEquals(1, cnt);

    }



    @Test
    public void updateDocuments() throws IOException {

        //System.out.println("\t\tAdd address and status to jdoe");
        //System.out.println("before :\t" + mainColl.findById(new HValue("jdoe")));

        Document doc = mainColl.findById(new HValue("jdoe"));
        assertNull(doc.getMap("address"));

        // create a mutation
        DocumentMutation mutation = new HDocumentMutation()
                .set("active", true)
                .set("address.line", "1015 15th Avenue")
                .set("address.city", "Redwood City")
                .set("address.zip", 94065);

        mainColl.update(new HValue("jdoe"), mutation);
        mainColl.flush();

        //System.out.println("after :\t\t" + mainColl.findById(new HValue("jdoe")));

        doc = mainColl.findById(new HValue("jdoe"));
        assertEquals(3, doc.getMap("address").size());


        //System.out.println("\n\n\t\tAppend new interests to users");

        doc = mainColl.findById(new HValue("jdoe"));
        assertNull(doc.getList("interests"));
        doc = mainColl.findById(new HValue("mdupont"));
        assertEquals(3, doc.getList("interests").size());

        // create a mutation
        mutation = new HDocumentMutation()
                .append("interests", Arrays.asList(new String[]{"development"}));

        mainColl.update(new HValue("jdoe"), mutation);
        mainColl.update(new HValue("mdupont"), mutation);
        mainColl.flush();

        //System.out.println("after :\t\t" + mainColl.findById(new HValue("jdoe"), "first_name", "last_name", "interests"));
        //System.out.println("after :\t\t" + mainColl.findById(new HValue("mdupont"), "first_name", "last_name", "interests"));

        doc = mainColl.findById(new HValue("jdoe"));
        assertEquals(1, doc.getList("interests").size());
        doc = mainColl.findById(new HValue("mdupont"));
        assertEquals(4, doc.getList("interests").size());


        //System.out.println("\n\n\t\tRemove attributes (dob)");
        //System.out.println("before :\t" + mainColl.findById(new HValue("jdoe")));

        doc = mainColl.findById(new HValue("jdoe"));
        assertEquals(0, doc.getDate("dob").compareTo(ODate.parse("1970-06-23")));

        // create a mutation
        mutation = new HDocumentMutation()
                .delete("dob");
        mainColl.update(new HValue("jdoe"), mutation);
        mainColl.flush();

        //System.out.println("after :\t\t" + mainColl.findById(new HValue("jdoe")));

        doc = mainColl.findById(new HValue("jdoe"));
        assertNull(doc.getDate("dob"));

    }

    @Test
    public void testPojo() {
        User2 user = new User2("jsmith", "John", "Smith");
        Document document = Json.newDocument(user);

        // save document into the table
        mainColl.insertOrReplace(document);

        user = mainColl.findById("jsmith").toJavaBean(User2.class);
        assertEquals("jsmith", user.getId());
        assertEquals("John", user.getFirstName());
        assertEquals("Smith", user.getLastName());

        mainColl.delete("jsmith");
    }

}
