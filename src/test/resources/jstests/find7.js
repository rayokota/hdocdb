
t = db.find7;
t.drop();

t.save({fn: "aaa", ln: "bbb", obj: {a: 1, b: "blah"}});

assert.eq(1, t.find({"obj.a": 1}, {obj: 1}).count(), "A")
assert.eq(1, t.find({obj: {a: 1, b: "blah"}}).count(), "B")
assert.eq(1, t.find({obj: {a: 1, b: "blah"}}, {obj: 1, _id: 0}).count(), "C")


