var HBaseConfiguration = Java.type("org.apache.hadoop.hbase.HBaseConfiguration")
var config = HBaseConfiguration.create();
var HDocumentDB = Java.type("io.hdocdb.store.HDocumentDB")
db = new HDocumentDB(config)

