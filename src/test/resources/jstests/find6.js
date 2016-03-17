
t = db.find6;
t.drop();

t.save( { a : 1 } );
t.save( { a : 1 , b : 1 } );

assert.eq( 2 , t.find().count() , "A" );
assert.eq( 1 , t.find( { b : null } ).count() , "B" );

/* test some stuff with dot array notation */
q = db.find6a;
q.drop();
q.save( { "a" : [ { "0" : 1 } ] } );
q.save( { "a" : [ { "0" : 2 } ] } );
q.save( { "a" : [ 1 ] } );
q.save( { "a" : [ 9, 1 ] } );

function f() {

    assert.eq( 2, q.find( { 'a[]' : 1 } ).count(), "da1");
    assert.eq( 2, q.find( { 'a[]' : 1 } ).count(), "da2");

    assert.eq( 1, q.find( { 'a[]' : { $gt : 8 } } ).count(), "da3");
    assert.eq( 0, q.find( { 'a[]' : { $lt : 0 } } ).count(), "da4");

}

f()

t = db.multidim;
t.drop();
t.save({"a" : [ [ ], 1, [ 3, 4 ] ] });
assert.eq(1, t.find({"a[2]":[3,4]}).count(), "md1");
assert.eq(1, t.find({"a[2][1]":4}).count(), "md2");
assert.eq(0, t.find({"a[2][1]":3}).count(), "md3");
