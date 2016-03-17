
t = db.getCollection( "basic1" );
t.drop();

o = { a : 1 };
t.save( o );

assert.eq( 1 , t.findOne().a , "first" );
assert( o._id , "now had id" );

o.a = 2;
t.save( o );

assert.eq( 2 , t.findOne().a , "second" );

