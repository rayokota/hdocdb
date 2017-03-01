# HDocDB - HBase as a JSON Document Database

HDocDB is a client layer for using HBase as a store for JSON documents.  It implements many of the interfaces in the [OJAI](http://ojai.github.io) framework.

## Installing

Releases of HDocDB are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.hdocdb</groupId>
    <artifactId>hdocdb</artifactId>
    <version>0.0.3</version>
</dependency>
```

## Building

You can also choose to build HDocDB manually.  Prerequisites for building:

* git
* Maven
* Java 8

```
git clone https://github.com/rayokota/hdocdb.git
cd hdocdb
mvn clean package -DskipTests
```

## Deployment

Currently HDocDB does not make use of coprocessors.  However, HDocDB does make use of server-side filters.  To deploy HDocDB:

* Add target/hdocdb-0.0.3.jar to the classpath of all HBase region servers.
* Restart the HBase region servers.
    

## Setup

To initialize HDocDB, an HBase connection is required.  For example,

```java
...
Configuration config = HBaseConfiguration.create();
Connection conn = ConnectionFactory.createConnection(config);
HDocumentDB hdocdb = new HDocumentDB(conn);
...
```

Next is to obtain a document collection. 

```java
...
HDocumentCollection coll = hdocdb.getCollection("mycollection");
...
```
		
Each document collection is backed by an HBase table.

## Creating Documents

Once a document collection is in hand, creating documents is straightforward.

```java
Document doc = new HDocument()
    .setId("jdoe")
    .set("firstName", "John")
    .set("lastName", "Doe")
    .set("dateOfBirth", ODate.parse("1970-10-10"));
coll.insert(doc);
```

You can also use the `insertOrReplace()` method, which will replace the document with the same ID if it already exists.

```java
coll.insertOrReplace(doc);
```

## Retrieving Documents

To retrieve all documents in a collection, use the `find()` method.

```java
DocumentStream docs = coll.find();
```
		
To retrieve a single document by ID, use the `findById()` method.

```java
Document doc = coll.findById("jdoe");
```
		
You can also pass a condition to the `find()` method.

```java
QueryCondition condition = new HQueryCondition()
    .and()
    .is("lastName", QueryCondition.Op.EQUAL, "Doe")
    .is("dateOfBirth", QueryCondition.Op.LESS, ODate.parse("1981-01-01"))
    .close()
    .build();
DocumentStream docs = coll.find(condition);
```

## Updating Documents

To update a document, first create a document mutation.

```java
DocumentMutation mutation = new HDocumentMutation()
    .setOrReplace("firstName", "Jim")
    .setOrReplace("dateOfBirth", ODate.parse("1970-10-09"));
coll.update("jdoe", mutation);
```
		
Here are the different types of methods supported with `HDocumentMutation`.

* `setOrReplace` - update or replace a field with the given value
* `set` - perform an update if a field either doesn't exist or has the same type as the given value
* `delete` - delete a field
* `increment` - increment a numeric field with the given value
* `append` - append the given array (or string) to an existing array (or string)
* `merge` - merge the given subdocument with an existing subdocument

All of the methods other than the `setOrReplace()` method perform a read-modify-write at the client side.

## Deleting Documents

To delete a document:

```java
coll.delete("jdoe");
```
		
## Saving and Retrieving Objects

Since OJAI has [Jackson](http://wiki.fasterxml.com/JacksonHome) integration, HDocDB can treat HBase as an object store.  Assuming your Java class is annotated as follows:

```java
public class User {

    private String id;
    private String firstName;
    private String lastName;

    @JsonCreator
    public User(@JsonProperty("_id")       String id,
                @JsonProperty("firstName") String firstName,
                @JsonProperty("lastName")  String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    @JsonProperty("_id")
    public String getId() { return id; }

    public String getFirstName() { return firstName; }

    public String getLastName() { return lastName; }
}
```

Then instances of your class can be saved and retrieved using HDocDB.
		
```java
User user = new User("jsmith", "John", "Smith");
Document doc = Json.newDocument(user);
coll.insert(doc);
...
user = coll.findById("jsmith").toJavaBean(User.class);
```
		
## Global Secondary Indexes

HDocDB also has basic support for global secondary indexes.  For more sophisticated indexing support, an engine that can perform full text searches, such as [ElasticSearch](https://www.elastic.co/products/elasticsearch) or [Solr](http://lucene.apache.org/solr/), is recommended.

Index management is performed mostly on the client-side, so it is not as performant as a coprocessor-based solution such as that provided by [Apache Phoenix](https://phoenix.apache.org).  Also, covered indexes are not supported, so each index lookup requires a join.  However, the currrent index implementation should still help speed up some reads (at the cost of slightly slower writes).

To create a secondary index on the `lastName` field:

```java
coll.createIndex("myindex" "lastName", Value.Type.STRING);
```
		
If the index is created after documents have already been added to the database, then the index will be populated in the background asynchronously.  Since the indexing is performed on the client, this may take some time for a large collection.

Now, when performing a query such as the following, the index above will be used.

```java
QueryCondition condition = new HQueryCondition()
    .and()
    .is("lastName", QueryCondition.Op.EQUAL, "Doe")
    .is("dateOfBirth", QueryCondition.Op.LESS, ODate.parse("1981-01-01"))
    .close()
    .build();
DocumentStream docs = coll.find(condition);
```

A query will use at most one index.  We can verify which index was used as follows.

```java
System.out.println(((HDocumentStream)docs).explain().asDocument());
```

which should print the following.

```json
{
    "plan": "index scan",
    "indexName": "myindex",
    "indexBounds": {"lastName": "[Doe‥Doe]"},
    "staleIndexesRunningCount": 0
}
```

We can also specify which index to use.

```java
DocumentStream docs = coll.findWithIndex("myindex", condition);
```
		
Or that no index should be used.

```java
DocumentStream docs = coll.findWithIndex(Index.NONE, condition);
```
		
You can also create compound indexes.

```java
IndexBuilder builder = coll.newIndexBuilder("myindex2")
    .add("lastName", Value.Type.STRING)
    .add("firstName", Value.Type.STRING)
    .build();
```


## HDocDB Shell with Nashorn Integration

The HDocDB shell is a command-line shell with [Nashorn](http://openjdk.java.net/projects/nashorn/) integration, so that MongoDB-like queries can be specified interactively or in a Nashorn script.

To start the HDocDB shell you need to use `jrunscript` that comes with Java (typically found in $JAVA_HOME/bin).

```
$ jrunscript -cp <hbase-conf-dir>:target/hdocdb-0.0.3.jar -f target/classes/shell/hdocdb.js -f - 
```

Here is a sample run.

```
nashorn> db.mycoll.insert( { _id: "jdoe", first_name: "John", last_name: "Doe" } )
	
nashorn> var doc = db.mycoll.find( { last_name: "Doe" } )[0]
	
nashorn> print(doc)
{"_id":"jdoe","first_name":"John","last_name":"Doe"}
	
nashorn> db.mycoll.update( { last_name: "Doe" }, { $set: { first_name: "Jim" } } )
	
nashorn> var doc = db.mycoll.find( { last_name: "Doe" } )[0]
	
nashorn> print(doc)
{"_id":"jdoe","first_name":"Jim","last_name":"Doe"}
	
nashorn> db.mycoll.delete( "jdoe" )
```

To run a script:

```
$ jrunscript -cp <hbase-conf-dir>:target/hdocdb-0.0.3.jar -f target/classes/shell/hdocdb.js -f <script>
```
	
## Implementation Notes

Each document is stored as a separate row in HBase.  This allows multiple operations on a document to be performed together atomically.  The document is essentially "shredded" using a technique called key-flattening, as described in the [Argo](http://pages.cs.wisc.edu/~chasseur/pubs/argo-long.pdf) paper.  That technique was developed for use with a relational database, but in HDocDB it has been [adapted](https://rayokota.wordpress.com/2016/03/17/hbase-as-a-multi-model-data-store/) for HBase.

The implementation of global secondary indexes is based on blogs by [Hofhansl](http://hadoop-hbase.blogspot.de/2012/10/musings-on-secondary-indexes.html) and [Yates](http://jyates.github.io/2012/07/09/consistent-enough-secondary-indexes.html).



