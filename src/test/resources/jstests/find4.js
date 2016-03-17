
t = db.find4;
t.drop();

t.save( { a : 1123  , b : 54332 } );

o = t.find( {} , {} )[0];
assert.eq( 1123 , o.a , "A" );
assert.eq( 54332 , o.b , "B" );

o = t.find( {} , { a : 1 } )[0];
assert.eq( 1123 , o.a , "D" );
assert( ! o.b , "F" );

o = t.find( {} , { b : 1 } )[0];
assert.eq( 54332 , o.b , "G" );
assert( ! o.a , "I" );

t.drop();
t.save( { a : 1 , b : 1 } );
t.save( { a : 2 , b : 2 } );
assert.eq( "1-1,2-2" , Array.prototype.map.call(t.find(), function(z){ return z.a + "-" + z.b; }).sort().toString() );
assert.eq( "1-null,2-null" , Array.prototype.map.call(t.find( {} , { a : 1 }), function(z){ return z.a + "-" + z.b; }).sort().toString() );
