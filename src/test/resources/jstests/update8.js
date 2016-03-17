
t = db.update8;
t.drop();

t.save( { _id : 1 } );
t.update( { _id : 1 }, {"$push": { tags : "a" } } );
assert.eq( { _id : 1 , tags : [ "a" ] } , t.findOne() , "A" );
