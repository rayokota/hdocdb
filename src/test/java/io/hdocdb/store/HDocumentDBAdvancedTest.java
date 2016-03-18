package io.hdocdb.store;

import com.google.common.collect.ImmutableList;
import io.hdocdb.HDocument;
import io.hdocdb.HDocumentBuilder;
import io.hdocdb.HDocumentStream;
import io.hdocdb.HValue;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.DocumentBuilder;
import org.ojai.DocumentStream;
import org.ojai.Value;
import org.ojai.Value.Type;
import org.ojai.store.DocumentMutation;
import org.ojai.store.QueryCondition;
import org.ojai.types.ODate;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

public class HDocumentDBAdvancedTest extends HDocumentDBTest {

    @BeforeClass
    public static void setup() throws IOException {
        HDocumentDBTest.setup();
    }

    @AfterClass
    public static void teardown() throws IOException {
        HDocumentDBTest.teardown();
    }

    @Test
    public void testDBDocumentWriterCRUD() throws IOException, Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        DocumentBuilder writer = this.createAndPrepareWriter();
        Document r = writer.getDocument();
        Assert.assertEquals("a string", r.getString("a.x"));
        coll.insertOrReplace(new HValue("key1"), r);
        coll.insertOrReplace(new HValue("key2"), r);
        r.set("p.q", 333456700L);
        coll.insertOrReplace(new HValue("key3"), r);
        coll.flush();
        Document newRec = coll.findById(new HValue("key1"));
        Assert.assertEquals("a string", newRec.getString("a.x"));
        Assert.assertEquals(999111666L, newRec.getLong("long"));
        Assert.assertEquals(true, newRec.getBoolean("map.bool"));

        newRec = coll.findById(new HValue("key2"));
        Assert.assertEquals(1234L, (long) newRec.getInt("map.array[2]"));
        Assert.assertEquals(OTimestamp.parse("2013-10-15T14:20:25.111-07:00"), newRec.getTimestamp("map.array[0]"));
        Assert.assertEquals(256L, (long) newRec.getShort("map.array[3].val2"));
        Assert.assertEquals(OTime.parse("07:30:35.999"), newRec.getTime("map.array[3].list[0]"));
        Assert.assertEquals(144.21D, (double) newRec.getFloat("record.inner.val1"), 1.0E-5D);
        coll.delete(new HValue("key2"));
        newRec = coll.findById(new HValue("key3"));
        Assert.assertEquals(333456700L, newRec.getLong("p.q"));
        Assert.assertEquals(111L, (long) newRec.getByte("map.array[1]"));
        newRec = coll.findById(new HValue("key2"));
        DocumentStream stream = coll.find();
        Iterator<Document> iter = stream.iterator();

        int count;
        for (count = 0; iter.hasNext(); ++count) {
            iter.next();
        }

        Assert.assertEquals(2L, (long) count);
        closeDocumentCollection(coll);
    }

    @Test
    public void testIdWithRecordWriter() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        DocumentBuilder writer = this.createAndPrepareWriter();
        Document rec = writer.getDocument();
        rec.set("_id", "k1");
        coll.insertOrReplace(rec);
        writer = this.createAndPrepareWriter();
        rec = writer.getDocument();
        rec.set("_id", "k2");
        coll.insertOrReplace(rec);
        Document newRec = coll.findById(new HValue("k1"));
        Assert.assertEquals(true, newRec.getBoolean("bool"));
        newRec = coll.findById(new HValue("k2"));
        Assert.assertEquals(new HValue("k2"), newRec.getValue("_id"));
        closeDocumentCollection(coll);
    }

    @Test
    public void recordMutTest2() throws IOException {
        Document r = getRecord();
        mainColl.insertOrReplace(new HValue("RecordMutKey"), r);
        Document var6 = mainColl.findById(new HValue("RecordMutKey"));
        Assert.assertEquals((long) var6.getInt("Scores[1]"), 20L);
        Assert.assertEquals(var6.getString("Friends[0]"), "Anurag");
        Assert.assertEquals((long) var6.getByte("map.byte"), 100L);
        Assert.assertEquals((long) var6.getShort("map.short"), 10000L);
        Assert.assertEquals(var6.getLong("map.long"), 12345678999L);
        Assert.assertEquals((long) var6.getInt("map.LIST2[3][2]"), 1500L);
        ByteBuffer buf = ByteBuffer.allocate(5000);

        for (int mutation = 0; mutation < 5000; ++mutation) {
            buf.put((byte) 55);
        }

        buf.rewind();
        DocumentMutation var7 = new HDocumentMutation();
        var7.increment("Scores[1]", 3.2D)
                .append("Friends[0]", " Choudhary")
                .increment("map.byte", 5.5D)
                .increment("map.short", 20000.2D)
                .delete("map.int")
                .delete("map.long")
                .setOrReplace("map.newfield", "THIS IS NEW")
                .increment("map.LIST2[3][2]", (byte) 120)
                .append("binary3", buf)
                .increment("NewField.field1.bytefield", (byte) 30)
                .increment("NewField.field1.shortfield", (short) 12345)
                .increment("NewField.field1.intfield", 567890123)
                .increment("NewField.field1.longfield", 7777777777777777L)
                .increment("NewField.field1.floatfield", 10.12345F)
                .increment("NewField.field1.doublefield", 111111.111111D);
        mainColl.update(new HValue("RecordMutKey"), var7);
        mainColl.flush();
        var6 = mainColl.findById(new HValue("RecordMutKey"));
        Assert.assertEquals((long) var6.getInt("Scores[1]"), 23L);
        Assert.assertEquals(var6.getString("Friends[0]"), "Anurag Choudhary");
        Assert.assertEquals((long) var6.getByte("map.byte"), 105L);
        Assert.assertEquals((long) var6.getShort("map.short"), 30000L);
        Assert.assertEquals(var6.getValue("map.int"), (Object) null);
        Assert.assertEquals(var6.getValue("map.long"), (Object) null);
        Assert.assertEquals(var6.getString("map.newfield"), "THIS IS NEW");
        Assert.assertEquals((long) var6.getInt("map.LIST2[3][2]"), 1620L);
        Assert.assertEquals(30L, (long) var6.getByte("NewField.field1.bytefield"));
        Assert.assertEquals(12345L, (long) var6.getShort("NewField.field1.shortfield"));
        Assert.assertEquals(567890123L, (long) var6.getInt("NewField.field1.intfield"));
        Assert.assertEquals(7777777777777777L, var6.getLong("NewField.field1.longfield"));
        Assert.assertEquals(10.12345027923584D, (double) var6.getFloat("NewField.field1.floatfield"), 0.0D);
        Assert.assertEquals(111111.111111D, var6.getDouble("NewField.field1.doublefield"), 0.0D);
        ByteBuffer b = var6.getBinary("binary3");

        int i;
        for (i = 0; i < 100; ++i) {
            Assert.assertEquals((long) b.get(i), (long) ((byte) i));
        }

        for (i = 100; i < 5000; ++i) {
            Assert.assertEquals((long) b.get(i), 55L);
        }
    }

    @Test
    public void testMultiCFGet() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document r = new HDocument();
        r.set("b", 0);
        r.set("a.b.x", 5L);
        r.set("a.b.c.d.e", 10L);
        coll.insertOrReplace(new HValue("key"), r);
        r = coll.findById(new HValue("key"), new String[]{"a.b"});
        Assert.assertEquals(5L, r.getLong("a.b.x"));
        Assert.assertEquals(10L, r.getLong("a.b.c.d.e"));
        Assert.assertNull(r.getValue("b"));
        r = coll.findById(new HValue("key"), new String[]{"a.b.c"});
        Assert.assertNull(r.getValue("a.b.x"));
        Assert.assertEquals(10L, r.getLong("a.b.c.d.e"));
        Assert.assertNull(r.getValue("b"));
        closeDocumentCollection(coll);
    }

    @Test(
            expected = Exception.class
    )
    public void mutateIncNonNumberTypes() throws IOException {
        Document r = new HDocument();
        r.set("Name", "Anurag");
        mainColl.insertOrReplace(new HValue("mutateIncNonNumberTypesKey"), r);
        r = mainColl.findById(new HValue("mutateIncNonNumberTypes"));
        Assert.assertEquals(r.getString("Name"), "Anurag");
        DocumentMutation mutation = new HDocumentMutation();
        mutation.increment("Name", 3.2D);
        mainColl.update(new HValue("RecordMutKey"), mutation);
        mainColl.flush();
    }

    @Test
    public void mutateSetOrReplaceConflictingPath() throws IOException {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document r = new HDocument();
        HValue key = new HValue("key");
        r.set("field1.field2[0]", 50).set("field1.field2[1]", 100).set("field1.field2[2]", 150);
        coll.insertOrReplace(key, r);
        r = coll.findById(new HValue("key"));
        Assert.assertEquals((long) r.getInt("field1.field2[0]"), 50L);
        Assert.assertEquals((long) r.getInt("field1.field2[1]"), 100L);
        Assert.assertEquals((long) r.getInt("field1.field2[2]"), 150L);
        DocumentMutation mutation = new HDocumentMutation();
        mutation.setOrReplace("field1.field2", 32);

        coll.update(key, mutation);
        coll.flush();
        r = coll.findById(new HValue("key"));
        Assert.assertEquals(r.getInt("field1.field2"), 32);

        mutation = new HDocumentMutation();
        mutation.setOrReplace("field1.field2.field3.field4", 32);
        coll.update(key, mutation);
        coll.flush();
        r = coll.findById(new HValue("key"));
        Assert.assertEquals(r.getInt("field1.field2.field3.field4"), 32);

        mutation = new HDocumentMutation();
        mutation.setOrReplace("field1.field2[5]", 32);
        coll.update(key, mutation);
        coll.flush();
        r = coll.findById(new HValue("key"));
        Assert.assertEquals(r.getInt("field1.field2[0]"), 32);
        closeDocumentCollection(coll);
    }

    @Test
    public void mutateIncConflictingPath() throws IOException {
        Document r = new HDocument();
        HValue key = new HValue("key");
        r.set("field1.field2[0]", 50).set("field1.field2[1]", 100).set("field1.field2[2]", 150);
        mainColl.insertOrReplace(key, r);
        r = mainColl.findById(new HValue("key"));
        Assert.assertEquals((long) r.getInt("field1.field2[0]"), 50L);
        Assert.assertEquals((long) r.getInt("field1.field2[1]"), 100L);
        Assert.assertEquals((long) r.getInt("field1.field2[2]"), 150L);
        DocumentMutation mutation = new HDocumentMutation();
        mutation.increment("field1.field2", 3.2D);
        boolean opFailed = false;

        try {
            mainColl.update(key, mutation);
            mainColl.flush();
        } catch (Exception e) {
            opFailed = true;
        }

        Assert.assertTrue(opFailed);
        mutation = new HDocumentMutation();
        mutation.increment("field1.field2.field3.field4", 3.2D);
        opFailed = false;

        try {
            mainColl.update(key, mutation);
            mainColl.flush();
        } catch (Exception e) {
            opFailed = true;
        }

        Assert.assertTrue(opFailed);
        mutation = new HDocumentMutation();
        mutation.increment("field1.field2[5]", 3.2D);
        opFailed = false;

        try {
            mainColl.update(key, mutation);
            mainColl.flush();
        } catch (Exception e) {
            opFailed = true;
        }

        Assert.assertTrue(opFailed);
    }

    @Test
    public void mutateIncLargeValue() throws IOException {
        Document r = new HDocument();
        long v = 1165513391666352928L;
        short inc = 1001;
        HValue key = new HValue("key");
        r.set("A", v).set("B", v);
        mainColl.insertOrReplace(key, r);
        r = mainColl.findById(key);
        Assert.assertEquals(r.getLong("A"), v);
        Assert.assertEquals(r.getLong("B"), v);
        DocumentMutation mutation = new HDocumentMutation();
        mutation.increment("A", inc).increment("B", inc);
        mainColl.update(key, mutation);
        mainColl.flush();
        r = mainColl.findById(key);
        Assert.assertEquals(r.getLong("A"), v + (long) inc);
        Assert.assertEquals(r.getLong("B"), v + (long) inc);
    }

    @Test
    public void mutateIncLongMAX() throws IOException {
        Document r = new HDocument();
        long v = 1165513391666352928L;
        long inc = Long.MAX_VALUE;
        HValue key = new HValue("key");
        r.set("byte", (byte) 100).set("short", (short) 12345).set("int", 12345678).set("long", v).set("float", 12345.123F).set("double", 1.111111111111111E8D);
        mainColl.insertOrReplace(key, r);
        r = mainColl.findById(key);
        DocumentMutation mutation = new HDocumentMutation();
        mutation.increment("byte", inc).increment("short", inc).increment("int", inc).increment("long", inc).increment("float", inc).increment("double", inc);
        mainColl.update(key, mutation);
        mainColl.flush();
        r = mainColl.findById(key);
        Assert.assertEquals((long) r.getByte("byte"), (long) ((byte) ((int) (100L + inc))));
        Assert.assertEquals((long) r.getShort("short"), (long) ((short) ((int) (12345L + inc))));
        Assert.assertEquals((long) r.getInt("int"), (long) ((int) (12345678L + inc)));
        Assert.assertEquals(r.getLong("long"), v + inc);
        Assert.assertEquals((double) r.getFloat("float"), (double) (12345.123F + (float) inc), 0.0D);
        Assert.assertEquals(r.getDouble("double"), 1.111111111111111E8D + (double) inc, 0.0D);
    }

    @Test
    public void mutateAppendTest() throws IOException {
        Document r = new HDocument();
        byte[] b = new byte[100];

        for (int key = 0; key < 100; ++key) {
            b[key] = (byte) key;
        }

        HValue key = new HValue("key");
        r.set("bytes", b).set("string", "Hello ").set("double1", 50.505D).set("double2", 50000.505D).setArray("array", new int[]{10, 20, 30, 40}).setArray("array2", new int[]{10, 20, 30, 40});
        mainColl.insertOrReplace(key, r);
        r = mainColl.findById(key);
        Assert.assertEquals("Hello ", r.getString("string"));
        Assert.assertEquals(10L, (long) r.getInt("array[0]"));
        Assert.assertEquals(20L, (long) r.getInt("array[1]"));
        Assert.assertEquals(30L, (long) r.getInt("array[2]"));
        Assert.assertEquals(40L, (long) r.getInt("array[3]"));
        Assert.assertEquals(10L, (long) r.getInt("array2[0]"));
        Assert.assertEquals(20L, (long) r.getInt("array2[1]"));
        Assert.assertEquals(30L, (long) r.getInt("array2[2]"));
        Assert.assertEquals(40L, (long) r.getInt("array2[3]"));
        Assert.assertEquals(50.505D, r.getDouble("double1"), 0.0D);
        Assert.assertEquals(50000.505D, r.getDouble("double2"), 0.0D);
        ByteBuffer bread = r.getBinary("bytes");

        for (int mutation = 0; mutation < 100; ++mutation) {
            Assert.assertEquals((long) ((byte) mutation), (long) bread.get());
        }

        DocumentMutation mutation = new HDocumentMutation();
        byte[] newBytes = new byte[50];

        for (int val = 0; val < 50; ++val) {
            newBytes[val] = -52;
        }

        Value var12 = r.getValue("string");
        Value val2 = r.getValue("double1");
        mutation.append("bytes", newBytes)
                .append("string", "world")
                .append("array", Arrays.asList(new Object[]{"W1", "W2", Integer.valueOf(500)}))
                .append("newpath.bytes", newBytes)
                .append("newpath.string", "hello world")
                .append("newpath.array", Arrays.asList(new Object[]{"W1", "W2", Integer.valueOf(500)}))
                .setOrReplace("newpath2.string", var12)
                .set("double2", val2)
                .setOrReplace("array2[1]", "NEW ARRAY ELEMENT")
                .setOrReplace("array2[2]", 5000.0D);
        mainColl.update(key, mutation);
        mainColl.flush();
        r = mainColl.findById(key);
        Assert.assertEquals("Hello world", r.getString("string"));
        Assert.assertEquals(10L, (long) r.getInt("array[0]"));
        Assert.assertEquals(20L, (long) r.getInt("array[1]"));
        Assert.assertEquals(30L, (long) r.getInt("array[2]"));
        Assert.assertEquals(40L, (long) r.getInt("array[3]"));
        Assert.assertEquals("W1", r.getString("array[4]"));
        Assert.assertEquals("W2", r.getString("array[5]"));
        Assert.assertEquals(500L, (long) r.getInt("array[6]"));
        Assert.assertEquals(10L, (long) r.getInt("array2[0]"));
        Assert.assertEquals("NEW ARRAY ELEMENT", r.getString("array2[1]"));
        Assert.assertEquals(5000.0D, r.getDouble("array2[2]"), 0.0D);
        Assert.assertEquals(40L, (long) r.getInt("array[3]"));
        bread = r.getBinary("bytes");

        int i;
        for (i = 0; i < 100; ++i) {
            Assert.assertEquals((long) ((byte) i), (long) bread.get());
        }

        for (i = 0; i < 50; ++i) {
            Assert.assertEquals(-52L, (long) bread.get());
        }

        bread = r.getBinary("newpath.bytes");

        for (i = 0; i < 50; ++i) {
            Assert.assertEquals(-52L, (long) bread.get());
        }

        Assert.assertEquals("hello world", r.getString("newpath.string"));
        Assert.assertEquals("Hello ", r.getString("newpath2.string"));
        Assert.assertEquals(50.505D, r.getDouble("double1"), 0.0D);
        Assert.assertEquals(50.505D, r.getDouble("double2"), 0.0D);
        Assert.assertEquals("W1", r.getString("newpath.array[0]"));
        Assert.assertEquals("W2", r.getString("newpath.array[1]"));
        Assert.assertEquals(500L, (long) r.getInt("newpath.array[2]"));
        Assert.assertEquals(500L, (long) r.getInt("newpath.array[2]"));
    }

    @Test
    public void testInsOrderWithMutInsert() throws IOException {
        Document r = null;
        Document putRec = (new HDocument()).set("m.n", (short) 1).set("m.m", (short) 2).set("m.q", (short) 3).set("m.c", (short) 31).set("m.d", (short) 32).set("m.e", (short) 33).set("m.f", (short) 34).set("m.q", (short) 35).set("m.x", (short) 13).set("m.a", (short) 7);
        mainColl.insertOrReplace(new HValue("CheckAndMutateKey"), putRec);
        DocumentMutation m = new HDocumentMutation().setOrReplace("m.j", (short) 8).delete("m.q").delete("m.x");
        QueryCondition c = new HQueryCondition().is("m.n", QueryCondition.Op.EQUAL, (short) 1).build();
        boolean conditionSuccess = mainColl.checkAndMutate(new HValue("CheckAndMutateKey"), c, m);
        Assert.assertTrue(conditionSuccess);
        r = mainColl.findById(new HValue("CheckAndMutateKey"));
        Assert.assertNotNull(r);
        m = new HDocumentMutation().increment("m.a", (short) 8);
        c = new HQueryCondition().is("m.m", QueryCondition.Op.EQUAL, (short) 2);
        conditionSuccess = mainColl.checkAndMutate(new HValue("CheckAndMutateKey"), c, m);
        Assert.assertTrue(conditionSuccess);
        r = mainColl.findById(new HValue("CheckAndMutateKey"));
        Assert.assertNotNull(r);
    }

    @Test
    public void checkAndMutateTest() throws IOException {
        Document r = getRecord();
        mainColl.insertOrReplace(new HValue("CheckAndMutateKey"), r);
        Document doc = mainColl.findById(new HValue("CheckAndMutateKey"));
        Assert.assertEquals((long) doc.getInt("Scores[1]"), 20L);
        Assert.assertEquals(doc.getString("Friends[0]"), "Anurag");
        Assert.assertEquals((long) doc.getByte("map.byte"), 100L);
        Assert.assertEquals((long) doc.getShort("map.short"), 10000L);
        Assert.assertEquals(doc.getLong("map.long"), 12345678999L);
        Assert.assertEquals((long) doc.getInt("map.LIST2[3][2]"), 1500L);
        ByteBuffer buf = ByteBuffer.allocate(5000);

        for (int mutation = 0; mutation < 5000; ++mutation) {
            buf.put((byte) 55);
        }

        buf.rewind();
        DocumentMutation mutation = new HDocumentMutation();
        mutation.increment("Scores[1]", 3.2D).append("Friends[0]", " Choudhary").increment("map.byte", 5.5D).increment("map.short", 20000.2D).delete("map.int").delete("map.long").setOrReplace("map.newfield", "THIS IS NEW").increment("map.LIST2[3][2]", (byte) 120).append("binary3", buf).increment("NewField.field1.field2", 50000L);
        QueryCondition c = new HQueryCondition();
        c.and().is("Friends[0]", QueryCondition.Op.EQUAL, "Anurag").is("map.long", QueryCondition.Op.LESS, 5L).close().build();
        boolean conditionSuccess = mainColl.checkAndMutate(new HValue("CheckAndMutateKey"), c, mutation);
        Assert.assertFalse(conditionSuccess);
        doc = mainColl.findById(new HValue("CheckAndMutateKey"));
        Assert.assertEquals((long) doc.getInt("Scores[1]"), 20L);
        Assert.assertEquals(doc.getString("Friends[0]"), "Anurag");
        Assert.assertEquals((long) doc.getByte("map.byte"), 100L);
        Assert.assertEquals((long) doc.getShort("map.short"), 10000L);
        Assert.assertEquals(doc.getLong("map.long"), 12345678999L);
        Assert.assertEquals((long) doc.getInt("map.LIST2[3][2]"), 1500L);
        c = new HQueryCondition();
        c.and().is("Friends[0]", QueryCondition.Op.EQUAL, "Anurag").is("map.long", QueryCondition.Op.GREATER, 5L).close().build();
        conditionSuccess = mainColl.checkAndMutate(new HValue("CheckAndMutateKey"), c, mutation);
        Assert.assertTrue(conditionSuccess);
        doc = mainColl.findById(new HValue("CheckAndMutateKey"));
        Assert.assertEquals((long) doc.getInt("Scores[1]"), 23L);
        Assert.assertEquals(doc.getString("Friends[0]"), "Anurag Choudhary");
        Assert.assertEquals((long) doc.getByte("map.byte"), 105L);
        Assert.assertEquals((long) doc.getShort("map.short"), 30000L);
        Assert.assertEquals(doc.getValue("map.int"), (Object) null);
        Assert.assertEquals(doc.getValue("map.long"), (Object) null);
        Assert.assertEquals(doc.getString("map.newfield"), "THIS IS NEW");
        Assert.assertEquals((long) doc.getInt("map.LIST2[3][2]"), 1620L);
        ByteBuffer b = doc.getBinary("binary3");

        int i;
        for (i = 0; i < 100; ++i) {
            Assert.assertEquals((long) b.get(i), (long) ((byte) i));
        }

        for (i = 100; i < 5000; ++i) {
            Assert.assertEquals((long) b.get(i), 55L);
        }

        mutation = new HDocumentMutation();
        mutation.increment("Scores[1]", 3.2D).set("Friends[0]", "Bharat").increment("map.byte", 5000);
        c = new HQueryCondition();
        c.and().is("Friends[0]", QueryCondition.Op.EQUAL, "Anurag Choudhary").is("map.short", QueryCondition.Op.GREATER, (short)5000).close().build();
        conditionSuccess = mainColl.checkAndMutate(new HValue("CheckAndMutateKey"), c, mutation);
        Assert.assertTrue(conditionSuccess);
        doc = mainColl.findById(new HValue("CheckAndMutateKey"));
        Assert.assertEquals((long) doc.getInt("Scores[1]"), 26L);
        Assert.assertEquals(doc.getString("Friends[0]"), "Bharat");
        Assert.assertEquals((long) doc.getShort("map.short"), 30000L);
        Assert.assertEquals(doc.getValue("map.int"), (Object) null);
        Assert.assertEquals(doc.getValue("map.long"), (Object) null);
        Assert.assertEquals(doc.getString("map.newfield"), "THIS IS NEW");
        Assert.assertEquals((long) doc.getInt("map.LIST2[3][2]"), 1620L);
        b = doc.getBinary("binary3");

        for (i = 0; i < 100; ++i) {
            Assert.assertEquals((long) b.get(i), (long) ((byte) i));
        }

        for (i = 100; i < 5000; ++i) {
            Assert.assertEquals((long) b.get(i), 55L);
        }

    }

    @Test
    public void checkAndDeleteTest() throws IOException {
        Document r = getRecord();
        mainColl.insertOrReplace(new HValue("CheckAndDeleteKey"), r);
        Document doc = mainColl.findById(new HValue("CheckAndDeleteKey"));
        Assert.assertEquals((long) doc.getInt("Scores[1]"), 20L);
        Assert.assertEquals(doc.getString("Friends[0]"), "Anurag");
        Assert.assertEquals((long) doc.getByte("map.byte"), 100L);
        Assert.assertEquals((long) doc.getShort("map.short"), 10000L);
        Assert.assertEquals(doc.getLong("map.long"), 12345678999L);
        Assert.assertEquals((long) doc.getInt("map.LIST2[3][2]"), 1500L);
        ByteBuffer buf = ByteBuffer.allocate(5000);

        for (int c = 0; c < 5000; ++c) {
            buf.put((byte) 55);
        }

        QueryCondition condition = new HQueryCondition();
        condition.and().is("Friends[0]", QueryCondition.Op.EQUAL, "Anurag").is("map.long", QueryCondition.Op.LESS, 5L).close().build();
        boolean conditionSuccess = mainColl.checkAndDelete(new HValue("CheckAndDeleteKey"), condition);
        Assert.assertFalse(conditionSuccess);
        doc = mainColl.findById(new HValue("CheckAndDeleteKey"));
        Assert.assertEquals((long) doc.getInt("Scores[1]"), 20L);
        Assert.assertEquals(doc.getString("Friends[0]"), "Anurag");
        Assert.assertEquals((long) doc.getByte("map.byte"), 100L);
        Assert.assertEquals((long) doc.getShort("map.short"), 10000L);
        Assert.assertEquals(doc.getLong("map.long"), 12345678999L);
        Assert.assertEquals((long) doc.getInt("map.LIST2[3][2]"), 1500L);
        condition = new HQueryCondition();
        condition.and().is("Friends[0]", QueryCondition.Op.EQUAL, "Anurag").is("map.long", QueryCondition.Op.GREATER, 5L).close().build();
        conditionSuccess = mainColl.checkAndDelete(new HValue("CheckAndDeleteKey"), condition);
        Assert.assertTrue(conditionSuccess);
        doc = mainColl.findById(new HValue("CheckAndDeleteKey"));
        Assert.assertEquals(doc, (Object) null);
    }

    @Test
    public void mergeTest() throws IOException {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document record1 = new HDocument().set("a", "b");
        Document record2 = new HDocument().set("x", record1);
        coll.insert(new HValue("key"), record2);
        Document doc = coll.findById(new HValue("key"));
        Assert.assertEquals(doc.getString("x.a"), "b");
        DocumentMutation mutation = new HDocumentMutation();
        Document record3 = new HDocument().set("c", "d");
        mutation.merge("x", record3);
        coll.update(new HValue("key"), mutation);
        doc = coll.findById(new HValue("key"));
        Assert.assertEquals(doc.getString("x.a"), "b");
        Assert.assertEquals(doc.getString("x.c"), "d");
        record2 = new HDocument().set("x", ImmutableList.of("hi", "bye"));
        coll.replace(new HValue("key"), record2);
        doc = coll.findById(new HValue("key"));
        Assert.assertEquals(doc.getString("x[0]"), "hi");
        Assert.assertEquals(doc.getString("x[1]"), "bye");
        mutation = new HDocumentMutation();
        record3 = new HDocument().set("c", "d");
        mutation.merge("x", record3);
        boolean opFailed = false;
        try {
            coll.update(new HValue("key"), mutation);
        } catch (Exception e) {
            opFailed = true;
        }
        Assert.assertTrue(opFailed);
        closeDocumentCollection(coll);
    }

    @Test
    public void replaceTest() throws IOException {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document record1 = new HDocument().set("a", "b");
        coll.insert(new HValue("record1"), record1);
        Document doc = coll.findById(new HValue("record1"));
        Assert.assertEquals(doc.getString("a"), "b");
        Document record2 = new HDocument().set("c", "d");
        coll.replace(new HValue("record1"), record2);
        doc = coll.findById(new HValue("record1"));
        Assert.assertEquals(doc.getString("a"), null);
        closeDocumentCollection(coll);
    }

    @Test
    public void checkAndReplaceTest() throws IOException {
        Document r = new HDocument().set("seq", 100).setArray("array", new int[]{1, 2, 3});
        mainColl.insertOrReplace(new HValue("CheckAndReplaceKey"), r);
        mainColl.flush();
        QueryCondition c = new HQueryCondition().is("seq", QueryCondition.Op.EQUAL, 100).build();
        r = new HDocument().set("seq", 101).setArray("array", new int[]{3, 4, 5});
        boolean conditionSuccess = mainColl.checkAndReplace(new HValue("CheckAndReplaceKey"), c, r);
        Assert.assertTrue(conditionSuccess);
        r = mainColl.findById(new HValue("CheckAndReplaceKey"));
        Assert.assertEquals((long) r.getInt("array[1]"), 4L);
        Document doc = getRecord();
        mainColl.insertOrReplace(new HValue("CheckAndReplaceKey"), doc);
        r = mainColl.findById(new HValue("CheckAndReplaceKey"));
        Assert.assertEquals((long) r.getInt("Scores[1]"), 20L);
        Assert.assertEquals(r.getString("Friends[0]"), "Anurag");
        Assert.assertEquals((long) r.getByte("map.byte"), 100L);
        Assert.assertEquals((long) r.getShort("map.short"), 10000L);
        Assert.assertEquals(r.getLong("map.long"), 12345678999L);
        Assert.assertEquals((long) r.getInt("map.LIST2[3][2]"), 1500L);
        ByteBuffer buf = ByteBuffer.allocate(5000);

        for (int newRecord = 0; newRecord < 5000; ++newRecord) {
            buf.put((byte) 55);
        }

        Document doc2 = new HDocument();
        doc2.set("user", "bill").set("address", "mountain view");
        c = new HQueryCondition();
        c.and().is("Friends[0]", QueryCondition.Op.EQUAL, "Anurag").is("map.long", QueryCondition.Op.LESS, 5L).close().build();
        conditionSuccess = mainColl.checkAndReplace(new HValue("CheckAndReplaceKey"), c, doc2);
        Assert.assertFalse(conditionSuccess);
        r = mainColl.findById(new HValue("CheckAndReplaceKey"));
        Assert.assertEquals((long) r.getInt("Scores[1]"), 20L);
        Assert.assertEquals(r.getString("Friends[0]"), "Anurag");
        Assert.assertEquals((long) r.getByte("map.byte"), 100L);
        Assert.assertEquals((long) r.getShort("map.short"), 10000L);
        Assert.assertEquals(r.getLong("map.long"), 12345678999L);
        Assert.assertEquals((long) r.getInt("map.LIST2[3][2]"), 1500L);
        c = new HQueryCondition();
        c.and().is("Friends[0]", QueryCondition.Op.EQUAL, "Anurag").is("map.long", QueryCondition.Op.GREATER, 5L).close().build();
        conditionSuccess = mainColl.checkAndReplace(new HValue("CheckAndReplaceKey"), c, doc2);
        Assert.assertTrue(conditionSuccess);
        r = mainColl.findById(new HValue("CheckAndReplaceKey"));
        Assert.assertEquals(r.getString("user"), "bill");
        Assert.assertEquals(r.getString("address"), "mountain view");
    }

    @Test
    public void testCheckAndReplaceMCF() throws IOException, Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        String field1 = "x.y.z";
        String field2 = "a.b.c.d.e";

        HashMap<String, String> cfNamePaths = new HashMap<>();
        cfNamePaths.put("cf1", field1);
        cfNamePaths.put("cf2", field2);
        Document subRecord = new HDocument().set("c", new HDocument().set("d", new HDocument().set("e", 1024)).set("f", "a.b.c")).set("g", "a.b");
        Document record1 = new HDocument().set("a", new HDocument().set("b", subRecord).set("h", "a")).set("i", "root");
        coll.insertOrReplace(new HValue("record1"), record1);
        QueryCondition condition = new HQueryCondition().and().notExists("x.y.z").is("a.b.c.d.e", QueryCondition.Op.NOT_EQUAL, 1023).close().build();
        Document newRecord = new HDocument().set("x.y.z", "x.y");
        Assert.assertTrue(coll.checkAndReplace(new HValue("record1"), condition, newRecord));
        closeDocumentCollection(coll);
    }

    @Test
    public void testMultiCFGetWithEmptyCF() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document r = new HDocument();
        r.set("a.b.c", 5);
        r.set("a.x", "aaa");
        r.set("a.b.x", "a.b.x");
        r.set("a.b.c.x", "a.b.c.x");
        coll.insertOrReplace(new HValue("key"), r);
        r = coll.findById(new HValue("key"));
        Assert.assertEquals("a.b.c.x", r.getString("a.b.c.x"));
        Assert.assertEquals("a.b.x", r.getString("a.b.x"));
        Assert.assertEquals("aaa", r.getString("a.x"));
        DocumentStream r2 = coll.find();
        Iterator<Document> iter = r2.iterator();

        while (iter.hasNext()) {
            Document r1 = iter.next();
            Assert.assertEquals("a.b.c.x", r1.getString("a.b.c.x"));
            Assert.assertEquals("a.b.x", r1.getString("a.b.x"));
            Assert.assertEquals("aaa", r1.getString("a.x"));
        }

        closeDocumentCollection(coll);
    }

    @Test
    public void testArrayDeletes() throws IOException, Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document record = new HDocument();
        ArrayList<Object> o1 = new ArrayList<>();
        o1.add(Integer.valueOf(1));
        o1.add("2");
        o1.add(Integer.valueOf(3));
        ArrayList<Object> o2 = new ArrayList<>();
        o2.add("A");
        o2.add("B");
        o1.add(o2);
        record.set("a.b", o1);
        record.set("a.c", "hello");
        coll.insertOrReplace(new HValue("recId"), record);
        coll.flush();
        DocumentMutation mutation = new HDocumentMutation();
        mutation.delete("a.b[0]");
        mutation.delete("a.b[1]");
        mutation.delete("a.b[2]");
        coll.update(new HValue("recId"), mutation);
        Document record1 = coll.findById(new HValue("recId"));
        closeDocumentCollection(coll);
    }

    @Test
    public void testMultiCF() throws IOException, Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document putRec = null;
        Document readRecordi = null;
        HashMap<String, String> cfPath = new HashMap<>();
        DocumentStream rs = null;
        Iterator<Document> itrs = null;
        DocumentMutation m = null;
        cfPath.put("cf1", "a.b");
        cfPath.put("cf2", "a.b.c");
        cfPath.put("cf3", "a");
        cfPath.put("cf4", "a.b.c.d");
        Document record1 = new HDocument().set("a", new HDocument().set("b", new HDocument().set("c", new HDocument()
                .set("d", new HDocument().set("e", "a.b.c.d")).set("f", "a.b.c")).set("g", "a.b")).set("h", "a")).set("i", "root");
        coll.insertOrReplace(new HValue("record1"), record1);
        coll.flush();
        readRecordi = coll.findById(new HValue("record1"));
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals("root", readRecordi.getString("i"));
        Assert.assertEquals("a.b.c.d", readRecordi.getString("a.b.c.d.e"));
        rs = coll.find();
        itrs = rs.iterator();

        while (itrs.hasNext()) {
            readRecordi = itrs.next();
            Assert.assertNotNull(readRecordi);
            Assert.assertEquals("root", readRecordi.getString("i"));
            Assert.assertEquals("a.b.c.d", readRecordi.getString("a.b.c.d.e"));
        }

        coll.close();
        cfPath = new HashMap<>();
        cfPath.put("cf1", "a");
        cfPath.put("cf2", "c");
        cfPath.put("ef3", "b");
        cfPath.put("ef4", "d");

        coll = getTempDocumentCollection();
        record1 = new HDocument().set("a", 1).set("b", 2).set("c", 3).set("d", 4).set("e", 5);
        coll.insertOrReplace(new HValue("MultiCF"), record1);
        readRecordi = coll.findById(new HValue("MultiCF"));
        Assert.assertNotNull(readRecordi);
        coll.close();
        cfPath = new HashMap<>();
        cfPath.put("f1", "l1");
        cfPath.put("f5", "l1.l2.l3.l4.l5");
        cfPath.put("f2", "l1.l2");
        cfPath.put("f3", "l1.l2.l3");
        cfPath.put("f4", "l1.l2.l3.l4");

        coll = getTempDocumentCollection();
        putRec = (new HDocument()).set("bool", true).set("l1.bool", true).set("l1.l2.bool", false).set("l1.l2.l3.bool", true).set("l1.l2.l3.l4.bool", false).set("l1.l2.l3.l4.l5.bool", false).set("l1.l2.l3.l4.l5.l6.l7.bool", true).set("l1.l2.l3.l4.l5.l6.l7.l8.bool", false).set("l1.l2.l3.l4.l5.l6.l7.l8.l9.bool", true);
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        readRecordi = coll.findById(new HValue("MultiCF"));
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals(true, readRecordi.getBoolean("bool"));
        Assert.assertEquals(true, readRecordi.getBoolean("l1.bool"));
        Assert.assertEquals(false, readRecordi.getBoolean("l1.l2.bool"));
        Assert.assertEquals(true, readRecordi.getBoolean("l1.l2.l3.bool"));
        Assert.assertEquals(false, readRecordi.getBoolean("l1.l2.l3.l4.bool"));
        putRec = (new HDocument()).set("a.b.c", "abc");
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        m = new HDocumentMutation().set("a.b.c", "cba");
        coll.update(new HValue("MultiCF"), m);
        readRecordi = coll.findById(new HValue("MultiCF"));
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals("cba", readRecordi.getString("a.b.c"));
        m = new HDocumentMutation().setOrReplace("a.b.c", "zyxcba");
        coll.update(new HValue("MultiCF"), m);
        readRecordi = coll.findById(new HValue("MultiCF"));
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals("zyxcba", readRecordi.getString("a.b.c"));
        cfPath = new HashMap<>();
        cfPath.put("f1", "p.q");
        cfPath.put("f2", "d.e");
        cfPath.put("f3", "a.b.c");

        coll = getTempDocumentCollection();
        putRec = (new HDocument()).set("a.b.c", "abc").set("m.n", "mn").set("d.e", "de").set("p.q", "pq");
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        readRecordi = coll.findById(new HValue("MultiCF"));
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals(Type.MAP, readRecordi.getValue("a.b").getType());
        Assert.assertEquals("mn", readRecordi.getString("m.n"));
        Assert.assertEquals("pq", readRecordi.getString("p.q"));
        Assert.assertEquals("abc", readRecordi.getString("a.b.c"));
        readRecordi = coll.findById(new HValue("MultiCF"), new String[]{"m.n", "a", "p.q", "a.b", "a.b.c", "d", "p"});
        Assert.assertNotNull(readRecordi);
        // RAY changed
        Assert.assertNotNull(readRecordi.getValue("d.e"));
        // RAY changed
        Assert.assertNotNull(readRecordi.getValue("p.q"));
        Assert.assertEquals("mn", readRecordi.getString("m.n"));
        Assert.assertEquals("pq", readRecordi.getString("p.q"));
        rs = coll.find();
        itrs = rs.iterator();

        int count;
        for (count = 0; itrs.hasNext(); ++count) {
            readRecordi = (Document) itrs.next();
            Assert.assertEquals("pq", readRecordi.getString("p.q"));
            Assert.assertEquals("mn", readRecordi.getString("m.n"));
            Assert.assertEquals("abc", readRecordi.getString("a.b.c"));
        }

        Assert.assertEquals(1L, (long) count);
        cfPath.clear();
        cfPath = new HashMap<>();
        cfPath.put("f1", "a.b");
        cfPath.put("f2", "a.b.c.d");
        coll = getTempDocumentCollection();
        putRec = (new HDocument()).set("a.b.c.d.e", "abcde");
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        readRecordi = coll.findById(new HValue("MultiCF"), new String[]{"a.b", "a.b.c", "a.b.c.d.e"});
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals(Type.MAP, readRecordi.getValue("a.b").getType());
        Assert.assertEquals("abcde", readRecordi.getString("a.b.c.d.e"));
        cfPath = new HashMap<>();
        cfPath.put("f1", "p.q");
        cfPath.put("f2", "d.e");
        cfPath.put("f3", "a.b.c");

        coll = getTempDocumentCollection();
        putRec = (new HDocument()).set("a.b.c", "abc").set("m.n", "mn").set("d.e", "de").set("p.q", "pq");
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        m = new HDocumentMutation().set("a.b.c", "cba").set("a.b.x", "xba").set("p.q", "qp");
        coll.update(new HValue("MultiCF"), m);
        readRecordi = coll.findById(new HValue("MultiCF"), new String[]{"a.b.c", "a.b.x"});
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals("cba", readRecordi.getString("a.b.c"));
        Assert.assertEquals("xba", readRecordi.getString("a.b.x"));
        m = new HDocumentMutation().setOrReplace("a.b.c", (short) 1111).setOrReplace("a.b.x", (short) 222).setOrReplace("p.q", (short) -32203);
        coll.update(new HValue("MultiCF"), m);
        readRecordi = coll.findById(new HValue("MultiCF"), new String[]{"a.b.c", "a.b.x"});
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals(1111L, (long) readRecordi.getShort("a.b.c"));
        Assert.assertEquals(222L, (long) readRecordi.getShort("a.b.x"));
        cfPath = new HashMap<>();
        cfPath.put("f1", "p.q");
        cfPath.put("f2", "d.e");
        cfPath.put("c3", "a.b.c");


        coll = getTempDocumentCollection();
        putRec = (new HDocument()).set("d.e", (short) 33).set("p.q", (short) 44).set("a.m", (short) 55).set("a.b.c", (short) 11).set("a.b.x", (short) 22);
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        readRecordi = coll.findById(new HValue("MultiCF"), new String[]{"p.q", "a.b"});
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals(22L, (long) readRecordi.getShort("a.b.x"));
        Assert.assertEquals(44L, (long) readRecordi.getShort("p.q"));
        m = new HDocumentMutation().increment("a.b.c", (short) 11).increment("a.b.x", (short) 22).increment("p.q", (short) 44);
        coll.update(new HValue("MultiCF"), m);
        readRecordi = coll.findById(new HValue("MultiCF"), new String[]{"a.b.c", "a.b.x", "a.m"});
        Assert.assertNotNull(readRecordi);
        Assert.assertEquals(22L, (long) readRecordi.getShort("a.b.c"));
        Assert.assertEquals(44L, (long) readRecordi.getShort("a.b.x"));

        coll = getTempDocumentCollection();
        putRec = (new HDocument()).set("a.b", (short) 11).set("p.q", (short) -32203);
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        readRecordi = coll.findById(new HValue("MultiCF"), new String[]{"a.b.c", "a.b"});
        Assert.assertNotNull(readRecordi);
        Assert.assertNull(readRecordi.getValue("a.b.c"));
        Assert.assertEquals(11L, (long) readRecordi.getShort("a.b"));
        cfPath = new HashMap<>();
        cfPath.put("f1", "p.q");
        cfPath.put("f2", "d.e");
        cfPath.put("f3", "a.b.c");

        coll = getTempDocumentCollection();
        putRec = (new HDocument()).set("a.b.c", (short) 111).set("p.q", (short) 222).set("d.e", (short) 333).set("a.b.x", (short) 444);
        coll.insertOrReplace(new HValue("MultiCF"), putRec);
        QueryCondition condition = new HQueryCondition().and().notExists("x.y.z").is("d.e", QueryCondition.Op.EQUAL, (short)333).close().build();
        rs = coll.find(condition, new String[]{"a.b.x", "a.b.c", "d"});
        itrs = rs.iterator();

        for (count = 0; itrs.hasNext(); ++count) {
            readRecordi = (Document) itrs.next();
            Assert.assertEquals(111L, (long) readRecordi.getShort("a.b.c"));
            Assert.assertEquals(444L, (long) readRecordi.getShort("a.b.x"));
            Assert.assertEquals(Type.MAP, readRecordi.getValue("d").getType());
        }

        Assert.assertEquals(1L, (long) count);
        readRecordi = coll.findById(new HValue("MultiCF"), condition, new String[]{"a.b.x", "a.b.c", "d"});
        Assert.assertEquals(111L, (long) readRecordi.getShort("a.b.c"));
        Assert.assertEquals(444L, (long) readRecordi.getShort("a.b.x"));
        Assert.assertEquals(Type.MAP, readRecordi.getValue("d").getType());
        closeDocumentCollection(coll);
    }

    @Test
    public void testMultiCF2() throws IOException, Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        HashMap<String, String> cfPath = new HashMap<>();
        cfPath.put("f1", "a.b");
        cfPath.put("f2", "b.c");
        Document r = new HDocument();
        r.set("a.h", "a").set("a.b.c.d.e", "a.b.c.d").set("a.b.c.f", "a.b.c").set("a.b.g", "a.b").set("b.c.f1", "f1").set("b.c.f2", "f2").set("i", "root");
        coll.insertOrReplace(new HValue("key"), r);
        coll.flush();
        r = coll.findById(new HValue("key"));
        DocumentMutation m = new HDocumentMutation();
        m.delete("a.b.c.d").delete("b.c");
        coll.update(new HValue("key"), m);
        coll.flush();
        r = coll.findById(new HValue("key"));
        Assert.assertEquals("a", r.getString("a.h"));
        Assert.assertEquals("a.b.c", r.getString("a.b.c.f"));
        Assert.assertEquals("a.b", r.getString("a.b.g"));
        Assert.assertEquals("root", r.getString("i"));
        Assert.assertNull(r.getValue("b.c"));
        Assert.assertNull(r.getValue("a.b.c.d"));
        closeDocumentCollection(coll);
    }

    @Test
    public void testIdRemoval() throws IOException, InterruptedException, Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        Document putRec = new HDocument();
        coll.insertOrReplace(new HValue("key"), putRec);
        coll.flush();
        Document r2 = coll.findById(new HValue("key"));
        Assert.assertEquals("key", r2.getId().getString());
        r2.delete("_id");
        Assert.assertNull(r2.getId());
        Assert.assertNull(r2.getValue("_id"));
        closeDocumentCollection(coll);
    }

    @Test
    public void testProjectionAndDeletes() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        Document putRec = getRecord();
        coll.insertOrReplace(new HValue("KEY1a"), putRec);
        coll.insertOrReplace(new HValue("KEY2a"), putRec);
        coll.insertOrReplace(new HValue("KEY3a"), putRec);
        Document readRecord = coll.findById(new HValue("KEY1a"));

        assert readRecord != null;

        coll.delete(new HValue("KEY1a"));
        readRecord = coll.findById(new HValue("KEY1a"));

        assert readRecord == null;

        readRecord = coll.findById(new HValue("KEY2a"), new String[]{"decimal", "map.long", "Friends", "map.Array2[1]"});
        Assert.assertEquals(readRecord.getId().getString(), "KEY2a");
        Map map = readRecord.asMap();
        Set keySet = map.keySet();
        Iterator i = keySet.iterator();
        Assert.assertEquals(i.next(), "_id");
        Assert.assertEquals(readRecord.getLong("map.long"), 12345678999L);
        Assert.assertEquals(readRecord.getString("Friends[0]"), "Anurag");
        Assert.assertEquals(readRecord.getString("Friends[1]"), "Bharat");
        Assert.assertEquals((long) readRecord.getInt("Friends[2]"), 10L);
        Assert.assertEquals(readRecord.getLong("map.Array2[0]"), -50000L);

        readRecord.setId("KEY4a");
        coll.insertOrReplace(new HValue("KEY4a"), readRecord);
        readRecord = coll.findById(new HValue("KEY4a"));
        Assert.assertEquals(readRecord.getLong("map.long"), 12345678999L);
        Assert.assertEquals(readRecord.getString("Friends[0]"), "Anurag");
        Assert.assertEquals(readRecord.getString("Friends[1]"), "Bharat");
        Assert.assertEquals((long) readRecord.getInt("Friends[2]"), 10L);
        Assert.assertEquals(readRecord.getLong("map.Array2[0]"), -50000L);
        coll.delete(new HValue("KEY2a"));
        coll.delete(new HValue("KEY3a"));
        coll.delete(new HValue("KEY4a"));
        closeDocumentCollection(coll);
    }

    @Test
    public void testProjectionWithIndex() throws Exception {
        HDocumentCollection coll;
        coll = getTempDocumentCollection();
        coll.createIndex("testindex", "map.string", Value.Type.STRING, Order.ASCENDING, false);
        Document putRec = getRecord();
        coll.insertOrReplace(new HValue("KEY1a"), putRec);

        int cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("map.string", QueryCondition.Op.EQUAL, "string"))) {
            Assert.assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                //System.out.println("\t" + doc);
            }
        }
        Assert.assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().and()
                .is("map.string", QueryCondition.Op.EQUAL, "string")
                .is("map.Array2[]", QueryCondition.Op.EQUAL, -50000L).close().build(),
                new String[]{"decimal", "map.long", "Friends", "map.Array2[1]"})) {
            Assert.assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                Assert.assertEquals(doc.getLong("map.Array2[0]"), -50000L);
                //System.out.println("\t" + doc);
            }
        }
        Assert.assertEquals(1, cnt);

        cnt = 0;
        try (DocumentStream documentStream = coll.find(new HQueryCondition().is("map.string", QueryCondition.Op.EQUAL, "string"),
                new String[]{"decimal", "map.long", "Friends", "map.Array2[1]"})) {
            Assert.assertEquals(((HDocumentStream)documentStream).explain().getIndexName(), "testindex");
            for (Document doc : documentStream) {
                cnt++;
                Assert.assertEquals(doc.getLong("map.long"), 12345678999L);
                Assert.assertEquals(doc.getString("Friends[0]"), "Anurag");
                Assert.assertEquals(doc.getString("Friends[1]"), "Bharat");
                Assert.assertEquals((long) doc.getInt("Friends[2]"), 10L);
                Assert.assertEquals(doc.getLong("map.Array2[0]"), -50000L);
                //System.out.println("\t" + doc);
            }
        }
        Assert.assertEquals(1, cnt);

        closeDocumentCollection(coll);
    }


    @Test
    public void booleanValueTest() throws Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        Document r1 = new HDocument().set("true", true).set("false", false).set("booleanarray[0]", false).set("booleanarray[1]", true).set("booleanarray[2]", true).set("booleanarray[3]", false);
        coll.insertOrReplace(new HValue("r1"), r1);
        Document r1get = coll.findById(new HValue("r1"));
        Assert.assertEquals(true, r1get.getBoolean("true"));
        Assert.assertEquals(false, r1get.getBoolean("false"));
        Assert.assertEquals(false, r1get.getBoolean("booleanarray[0]"));
        Assert.assertEquals(true, r1get.getBoolean("booleanarray[1]"));
        Assert.assertEquals(true, r1get.getBoolean("booleanarray[2]"));
        Assert.assertEquals(false, r1get.getBoolean("booleanarray[3]"));
        closeDocumentCollection(coll);
    }

    @Test
    public void nullValueTest() throws Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        Document r1 = new HDocument().set("level0_a3_array", Arrays.asList(new String[]{null, "A"})).setNull("level0_a4_long");
        coll.insertOrReplace(new HValue("r1"), r1);
        Document r1get = coll.findById(new HValue("r1"));
        Assert.assertEquals(Type.NULL, r1get.getValue("level0_a3_array[0]").getType());
        Assert.assertEquals("A", r1get.getString("level0_a3_array[1]"));
        Assert.assertEquals(Type.NULL, r1get.getValue("level0_a4_long").getType());
        closeDocumentCollection(coll);
    }

    @Test
    public void existsTest() throws Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        Document r = new HDocument();
        r.set("f1", "abc");
        coll.insertOrReplace(new HValue("exists"), r);
        int count = ((HDocumentStream)coll.find(new HQueryCondition().exists("f1"))).count();
        Assert.assertEquals(1, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().notExists("f1"))).count();
        Assert.assertEquals(0, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().exists("f2"))).count();
        Assert.assertEquals(0, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().notExists("f2"))).count();
        Assert.assertEquals(1, count);
        closeDocumentCollection(coll);
    }

    @Test
    public void inTest() throws Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        Document r = new HDocument();
        r.set("f1", "abc");
        coll.insertOrReplace(new HValue("in"), r);
        int count = ((HDocumentStream)coll.find(new HQueryCondition().in("f1", ImmutableList.of("abc", "def")))).count();
        Assert.assertEquals(1, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().in("f1", ImmutableList.of("ghi", "def")))).count();
        Assert.assertEquals(0, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().notIn("f1", ImmutableList.of("abc", "def")))).count();
        Assert.assertEquals(0, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().notIn("f1", ImmutableList.of("ghi", "def")))).count();
        Assert.assertEquals(1, count);
        closeDocumentCollection(coll);
    }

    @Test
    public void typeTest() throws Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        Document r = new HDocument();
        r.set("f1", "abc");
        coll.insertOrReplace(new HValue("type"), r);
        int count = ((HDocumentStream)coll.find(new HQueryCondition().typeOf("f1", Type.STRING))).count();
        Assert.assertEquals(1, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().typeOf("f1", Type.BINARY))).count();
        Assert.assertEquals(0, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().notTypeOf("f1", Type.STRING))).count();
        Assert.assertEquals(0, count);
        count = ((HDocumentStream)coll.find(new HQueryCondition().notTypeOf("f1", Type.BINARY))).count();
        Assert.assertEquals(1, count);
        closeDocumentCollection(coll);
    }

    @Test
    public void testIncrement() throws Exception {
        mainColl.increment(new HValue("K1"), "a.byte", (byte) 100);
        mainColl.increment(new HValue("K1"), "a.short", (short) 100);
        mainColl.increment(new HValue("K1"), "a.int", 100);
        mainColl.increment(new HValue("K1"), "a.long", 100L);
        mainColl.increment(new HValue("K1"), "a.float", 100.1F);
        mainColl.increment(new HValue("K1"), "a.double", 100.1D);
        mainColl.flush();
        Document r = mainColl.findById(new HValue("K1"));
        Assert.assertEquals(100L, (long) r.getByte("a.byte"));
        Assert.assertEquals(100L, (long) r.getShort("a.short"));
        Assert.assertEquals(100L, (long) r.getInt("a.int"));
        Assert.assertEquals(100L, r.getLong("a.long"));
        Assert.assertEquals(100.0999984741211D, (double) r.getFloat("a.float"), 0.0D);
        Assert.assertEquals(100.1D, r.getDouble("a.double"), 0.0D);
    }

    @Test
    public void arrayDeleteTest() throws Exception {
        Document r = new HDocument();
        r.set("a.b[0]", 1).set("a.b[1]", "2").set("a.b[2]", 3).set("a.b[3][0]", "A").set("a.b[3][1]", "B").set("a.b[3][2]", "C");
        mainColl.insertOrReplace(new HValue("arraydeletekey"), r);
        mainColl.flush();
        Document r2 = mainColl.findById(new HValue("arraydeletekey"));
        Assert.assertEquals(r.setId("arraydeletekey"), r2);
        DocumentMutation m = new HDocumentMutation();
        m.append("a.b[1]", "dummystring1");
        m.append("a.b[3][0]", "dummystring2");
        m.append("a.b[3][1]", "dummystring3");
        mainColl.update(new HValue("arraydeletekey"), m);
        Document r3 = mainColl.findById(new HValue("arraydeletekey"));
        Assert.assertEquals("2dummystring1", r3.getString("a.b[1]"));
        Assert.assertEquals("Adummystring2", r3.getString("a.b[3][0]"));
        Assert.assertEquals("Bdummystring3", r3.getString("a.b[3][1]"));
    }

    @Test
    public void docAsValueTest() throws Exception {
        Document r = new HDocument();
        r.set("Name", "XXX");
        mainColl.insertOrReplace(new HValue("key"), r);
        mainColl.flush();
        Document r2 = new HDocument().set("type", "Home").set("street", "350 River Oaks").set("city", "sanjose");
        DocumentMutation m = new HDocumentMutation();
        m.setOrReplace("address", r2);
        m.append("addresses", Arrays.asList(new Object[]{r2}));
        mainColl.update(new HValue("key"), m);
        Document d3 = mainColl.findById(new HValue("key"));
        Assert.assertEquals("Home", d3.getString("address.type"));
        Assert.assertEquals("350 River Oaks", d3.getString("address.street"));
        Assert.assertEquals("sanjose", d3.getString("address.city"));
        Assert.assertEquals("Home", d3.getString("addresses[0].type"));
        Assert.assertEquals("350 River Oaks", d3.getString("addresses[0].street"));
        Assert.assertEquals("sanjose", d3.getString("addresses[0].city"));
    }

    @Test
    public void setNullTest() throws Exception {
        Document r = new HDocument();
        r.set("f1", "abc");
        mainColl.insertOrReplace(new HValue("nullkey1"), r);
        mainColl.flush();
        DocumentMutation m = new HDocumentMutation();
        m.set("f2", 1000).setNull("f3");
        mainColl.update(new HValue("nullkey1"), m);
        mainColl.flush();
        Document d = mainColl.findById(new HValue("nullkey1"));
        Assert.assertEquals(1000L, (long) d.getInt("f2"));
        Assert.assertEquals(Type.NULL, d.getValue("f3").getType());
    }

    @Test
    public void setOrReplaceNullTest() throws Exception {
        Document r = new HDocument();
        r.set("f1", "abc");
        mainColl.insertOrReplace(new HValue("nullkey1"), r);
        mainColl.flush();
        DocumentMutation m = new HDocumentMutation();
        m.set("f2", 1000).setOrReplaceNull("f3");
        mainColl.update(new HValue("nullkey1"), m);
        mainColl.flush();
        Document d = mainColl.findById(new HValue("nullkey1"));
        Assert.assertEquals(1000L, (long) d.getInt("f2"));
        Assert.assertEquals(Type.NULL, d.getValue("f3").getType());
    }

    @Test
    public void testNestedCFProj() throws Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        HashMap<String, String> cfPath = new HashMap<>();
        cfPath.put("cf1", "a.b.c.d");
        cfPath.put("cf2", "a.b");
        Document doc = new HDocument();
        doc.set("a.b.h", "abh").set("a.b.c.g", "abcg").set("a.b.c.d.e", "abcde").set("a.b.c.d.f", "abcdf").set("a.b.i", "abi").set("i", "i");
        coll.insertOrReplace(new HValue("order"), doc);
        Document readDoc = null;
        readDoc = coll.findById(new HValue("order"), new String[]{"a.b.c.g"});
        Assert.assertEquals("abcg", readDoc.getString("a.b.c.g"));
        Assert.assertNull(readDoc.getValue("a.b.c.d"));
        readDoc = coll.findById(new HValue("order"), new String[]{"a.b.h"});
        Assert.assertEquals("abh", readDoc.getString("a.b.h"));
        Assert.assertNull(readDoc.getValue("a.b.c.d"));
        readDoc = coll.findById(new HValue("order"), new String[]{"a.b"});
        Assert.assertNull(readDoc.getValue("i"));
        Assert.assertEquals("abcg", readDoc.getString("a.b.c.g"));
        Assert.assertEquals("abcde", readDoc.getString("a.b.c.d.e"));
        readDoc = coll.findById(new HValue("order"), new String[]{"a.b.c.d.e"});
        Assert.assertNull(readDoc.getValue("a.b.c.g"));
        Assert.assertEquals("abcde", readDoc.getString("a.b.c.d.e"));
        Assert.assertNull(readDoc.getValue("a.b.h"));
        readDoc = coll.findById(new HValue("order"), new String[]{"a.b.c"});
        Assert.assertEquals("abcg", readDoc.getString("a.b.c.g"));
        Assert.assertEquals("abcde", readDoc.getString("a.b.c.d.e"));
        Assert.assertNull(readDoc.getValue("a.b.i"));
        Assert.assertNull(readDoc.getValue("a.b.h"));
        Assert.assertNull(readDoc.getValue("i"));
        readDoc = coll.findById(new HValue("order"), new String[]{""});
        Assert.assertEquals("abcg", readDoc.getString("a.b.c.g"));
        Assert.assertEquals("abcde", readDoc.getString("a.b.c.d.e"));
        Assert.assertEquals("abi", readDoc.getString("a.b.i"));
        Assert.assertEquals("i", readDoc.getString("i"));
        closeDocumentCollection(coll);
    }

    @Test
    public void testFieldOrderMultiCF() throws Exception {
        HDocumentCollection coll = getTempDocumentCollection();
        HashMap<String, String> cfPath = new HashMap<>();
        cfPath.put("cf1", "a.b");
        cfPath.put("cf2", "x.y");
        cfPath.put("cf3", "stairway_to_level1.stairway_to_level2");
        Document doc = new HDocument();
        doc.set("a.b.l1", "1").set("a.b.l4.a", "11").set("a.b.l4.b", "12").set("a.b.l5", "5");
        coll.insertOrReplace(new HValue("order"), doc);
        Document readDoc = coll.findById(new HValue("order"));
        Map map = readDoc.getMap("");
        Map putMap = doc.setId(new HValue("order")).getMap("");
        Assert.assertEquals(putMap, map);
        closeDocumentCollection(coll);
    }

    private DocumentBuilder createAndPrepareWriter() {
        Document innerRecord = new HDocument();
        innerRecord.set("val1", 144.21F);
        innerRecord.set("val2", (short) 256);
        ArrayList<Object> l = new ArrayList<>();
        l.add(OTime.parse("07:30:35.999"));
        l.add(new BigDecimal(4444.1928282D));
        innerRecord.set("list", l);
        DocumentBuilder writer = new HDocumentBuilder();
        writer.addNewMap();
        writer.put("a.x", "a string");
        writer.put("bool", true);
        writer.put("long", 999111666L);
        writer.putNewMap("map");
        writer.put("bool", true);
        writer.put("date", ODate.parse("2013-12-12"));
        writer.putNewArray("array");
        writer.add(OTimestamp.parse("2013-10-15T14:20:25.111-07:00"));
        writer.add((byte) 111);
        writer.add(1234);
        writer.add(innerRecord);
        writer.endArray();
        writer.put("float", 123.456F);
        writer.endMap();
        writer.put("record.inner", innerRecord);
        writer.endMap();
        return writer;
    }

    private static Document getRecord() {
        Document rec = new HDocument();
        rec.setArray("Scores", new int[]{10, 20, 30})
                .setArray("Friends", new Object[]{"Anurag", "Bharat", new Integer(10)})
                .set("map.boolean", true)
                .set("map.string", "string")
                .set("map.byte", (byte) 100)
                .set("map.short", (short) 10000)
                .set("map.int", '썐')
                .set("map.long", 12345678999L)
                .set("map.float", 10.1234F)
                .set("map.double", 10.1234567891D)
                .setArray("map.Array2", new Object[]{new Double("-2321232.1234312"), new Long(-50000L), new Integer(10)})
                .setNull("NULL");
        ByteBuffer bbuf = ByteBuffer.allocate(100);

        for (int values2 = 0; values2 < bbuf.capacity(); ++values2) {
            bbuf.put((byte) values2);
        }

        bbuf.rewind();
        rec.set("binary3", bbuf).set("Time", new OTime(10000000L)).set("Date", new ODate(432000000L));
        rec.set("boolean", false);
        rec.set("string", "stringstrinstringstring");
        rec.set("byte", (byte) 100);
        List<Object> values = new ArrayList<>();
        values.add("Field1");
        values.add(new Integer(500));
        values.add(new Double(5555.5555D));
        rec.set("map.LIST", values);
        ArrayList<Object> var4 = new ArrayList<>();
        var4.add("Field1");
        var4.add(new Integer(500));
        var4.add(new Double(5555.5555D));
        var4.add(new int[]{500, 1000, 1500, 2000});
        rec.set("map.LIST2", var4);
        rec.set("NAME", "ANURAG");
        boolean[] ba = new boolean[]{false, true, true};
        rec.setArray("map.boolarray", ba);
        return rec;
    }

}
