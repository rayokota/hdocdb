
t = db.update_arraymatch3;
t.drop();

o = { _id : 1 , 
      comments : [ { "by" : "joe", "votes" : 3 },
                   { "by" : "jane", "votes" : 7 } 
                 ],
      title : "ABC"
    };

t.save( o );
assert.eq( o , t.findOne() , "A1" );

t.update( {'comments[].by':'joe'}, {$inc:{'comments[0].votes':1}}, true );
o.comments[0].votes++;
assert.eq( o , t.findOne() , "A2" );
