
t = db.find5;
t.drop();

t.save({_id: 1, a: 1});
t.save({_id: 2, b: 5});

assert.eq( 2 , t.find({}, {b:1}).count(), "A");

x = t.find( {} , { a : 1 } )[0];
assert.eq( 1 , x.a , "C1" );
assert.isnull( x.b , "C2" );

x = t.find( {} , { a : 1 } )[1];
assert.isnull( x.a , "C3" );
assert.isnull( x.b , "C4" );

x = t.find( {} , { b : 1 } )[0];
assert.isnull( x.a , "C5" );
assert.isnull( x.b , "C6" );

x = t.find( {} , { b : 1 } )[1];
assert.isnull( x.a , "C7" );
assert.eq( 5 , x.b , "C8" );

t.drop();


t.save( { a : 1 , b : { c : 2 , d : 3 , e : 4 } } );
assert.eq( 2 , t.find( {} , { "b.c" : 1 } ).toArray()[0].b.c , "D" );

o = t.find( {} , { "b.c" : 1 , "b.d" : 1 } ).toArray()[0];
assert( o.b.c , "E 1" );
assert( o.b.d , "E 2" );
assert( !o.b.e , "E 3" );

assert( ! t.find( {} , { "b.c" : 1 } ).toArray()[0].b.d , "F" );

t.drop();
t.save( { a : { b : { c : 1 } } } );
assert.eq( 1 , t.find( {} , { "a.b.c" : 1 } )[0].a.b.c , "G" );

