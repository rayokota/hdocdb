db.jstest.insertOrReplace({ _id: "id3",  a: 1, b: 2.5, c: new Date(), d: 1.234, e: true, f: "hi", g: [ 1,2], h: {a: 1, b: 2} })
var doc = db.jstest.findById("id3")
print("found " + doc)
db.jstest.update({ _id: "id3"}, { $set: { b: 3.5 }})
db.jstest.update({ _id: "id2"}, { $set: { a: 4.5 }})
var stream = db.jstest.find()
for each (var doc in stream) print(doc)
var stream = db.jstest.find( { $or: [ { b: 3.5 }, { a: 4.5 } ] } )
for each (var doc in stream) print(doc)





