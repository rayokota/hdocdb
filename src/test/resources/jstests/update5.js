
t = db.update5;

function go( key ){

    t.drop();

    function check( num , name ){
        assert.eq( 1 , t.find().count() , tojson( key ) + " count " + name );
        assert.eq( num , t.findOne().n , tojson( key ) +  " value " + name );
    }

    t.save( key );

    t.update( key , { $inc : { n : 1 } } );
    check( 1 , "A" );
    
    t.update( key , { $inc : { n : 1 } } );
    check( 2 , "B" );
    
    t.update( key , { $inc : { n : 1 } } );
    check( 3 , "C" );
    
    t.update( key , { $inc : { n : 1 } } );
    check( 4 , "D" );
    
}

go( { a : 5 } );
go( { a : 5 } );

go( { a : 5 , b : 7 } );
go( { a : null , b : 7 } );

go( { referer: 'blah' } );
go( { referer: 'blah', lame: 'bar' } );
go( { referer: 'blah', name: 'bar' } );
go( { date: null, referer: 'blah', name: 'bar' } );
