
var res;
t = db.updatea;
t.drop();

orig = { _id : 1 , a : [ { x : 1 , y : 2 } , { x : 10 , y : 11 } ] };

res = t.save( orig );

res = t.update( {} , { $set : { "a[0].x" : 3 } } );
orig.a[0].x = 3;
assert.eq( orig , t.findOne() , "A1" );

res = t.update( {} , { $set : { "a[1].z" : 17 } } );
orig.a[1].z = 17;
assert.eq( orig , t.findOne() , "A2" );

res = t.update( {} , { $unset : { "a[1].y" : 1 } } );
delete orig.a[1].y;
assert.eq( orig , t.findOne() , "A3" );

t.drop();
orig = { _id : 1 , comments : [ { name : "blah" , rate_up : 0 , rate_ups : [] } ] };
res = t.save( orig );


res = t.update( {} , { $inc: { "comments[0].rate_up" : 1 } , $push: { "comments[0].rate_ups" : 99 } } );
orig.comments[0].rate_up++;
orig.comments[0].rate_ups.push( 99 );
assert.eq( orig , t.findOne() , "B1" );

t.drop();
orig = { _id : 1 , a : [] };
for ( i=0; i<12; i++ )
    orig.a.push( i );


res = t.save( orig );
assert.eq( orig , t.findOne() , "C1" );

res = t.update( {} , { $inc: { "a[0]" : 1 } } );
orig.a[0]++;
assert.eq( orig , t.findOne() , "C2" );

res = t.update( {} , { $inc: { "a[10]" : 1 } } );
orig.a[10]++;


t.drop();
t.save({"a":{"c00":1}, 'c':2});
res = t.update({"c":2}, {'$inc':{'a.c000':1}});

assert.eq( { "c00" : 1 , "c000" : 1 } , t.findOne().a , "D1" );

