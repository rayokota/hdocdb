f = db.ed_db_update2;

f.drop();
f.save( { a: 4 } );
f.update( { a: 4 }, { $inc: { a: 2 } } );
assert.eq( 6, f.findOne().a );
