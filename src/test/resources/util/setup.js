var db;
if (!useMock) {
  print("Using HBase...")
  var Configuration = Java.type("org.apache.hadoop.conf.Configuration")
  var config = new Configuration()
  config.set("hbase.zookeeper.quorum", "127.0.0.1")
  config.set("zookeeper.znode.parent", "/hbase-unsecure")
  var HDocumentDB = Java.type("io.hdocdb.store.HDocumentDB")
  db = new HDocumentDB(config)
} else {
  print("Using mock...")
  var MockHDocumentDB = Java.type("io.hdocdb.store.MockHDocumentDB")
  var MockHDocDB = Java.extend(MockHDocumentDB)
  db = new MockHDocDB() {
    save: function(doc) {
      insert(doc)
    }
  }
}
