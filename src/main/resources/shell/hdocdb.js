var HBaseConfiguration = Java.type("org.apache.hadoop.hbase.HBaseConfiguration")
var config = HBaseConfiguration.create();
var ConnectionFactory = Java.type("org.apache.hadoop.hbase.client.ConnectionFactory")
var conn = ConnectionFactory.createConnection(config)
var HDocumentDB = Java.type("io.hdocdb.store.HDocumentDB")
db = new HDocumentDB(conn)

