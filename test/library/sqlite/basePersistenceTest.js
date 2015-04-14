var q = require('q');

exports.testSave = function(testUtils, persistence, collectionID ) {
    var deferred = q.defer();

    collectionID = collectionID || testUtils.guid();

    var obj = {
        'data' : testUtils.guid()
    };

    try {
        persistence.save(collectionID, testUtils.guid(), obj ).then( function(saved) {
            if ( !saved ) {
                deferred.reject(new Error('Did not save object'));
            }

            if ( saved['data'] !== obj['data'] ) {
                deferred.reject(new Error('Data did not persist'));
            } else {
                deferred.resolve();
            }
        }, function(e) {
            deferred.reject(e);
        });
    } catch( e ) {
        deferred.reject(e);
    }

    return deferred.promise;
};

exports.testFind = function( testUtils, persistence ) {
    var deferred = q.defer();

    try {

        function pretty(result) {
            return '\n' + JSON.stringify(result, null, 4) + '\n';
        }

        // setup some data
        persistence.save(               'myCollection', '123', { 'key': '1', 'data': { 'name': 'aron', 'age': '6' } } )

            .then( function( ) {
                return persistence.save('myCollection', '456', { 'key': '2', 'data': { 'name': 'allen' } });
            })
            .then( function( ) {
                return persistence.save('myCollection', '789', { 'key': '3', 'data': { 'name': 'abbey', 'age' : '6' } });
            })
            .then( function( ) {
                return persistence.save('myOtherCollection', '1', { 'data' : { 'number' : 1 } } );
            })
            .then( function( ) {
                return persistence.save('myOtherCollection', '2', { 'data' : { 'number' : 2 } } );
            })
            .then( function( ) {
                return persistence.save('myOtherCollection', '3', { 'data' : { 'number' : 3 } } );
            })
            .then( function( ) {
                return persistence.save('myOtherCollection', '4', { 'data' : { 'number' : 4 } } );
            })
            .then( function( ) {
                return persistence.save('myOtherCollection', '5', { 'data' : { 'number' : 5 } } );
            })
            .then( function( ) {
                return persistence.save('counter', 'countValue', 1000 );
            })

            // find by ID
            .then( function( ) {
                return persistence.findByID('myCollection', '789').then( function(result) {
                    if ( !result ) {
                        deferred.reject('Failed find by ID');
                    }
                    if ( result['data']['name'] !== 'abbey') {
                        deferred.reject('Failed find by ID, found wrong result');
                    }
                });
            })
            // find by ID
            .then( function( ) {
                return persistence.findByID('counter', 'countValue').then( function(result) {
                    if ( !result ) {
                        deferred.reject('Failed find by ID');
                    }
                    if ( result !== 1000) {
                        deferred.reject('Failed find by ID, found wrong result');
                    }
                });
            })

            .then( function( ) {
                return persistence.find('myCollection',
                    { 'key' : '2' }
                ).then( function(result) {
                        if ( !result ) {
                            deferred.reject('Find by key search');
                        }
                        if ( result.length != 1 ) {
                            deferred.reject('Find by key search');
                        }
                        if ( result[0]['data']['name'] !== 'allen') {
                            deferred.reject('Failed find by ID, found wrong result');
                        }
                    });
            })

            .then( function( ) {
                return persistence.find('myCollection',
                    { 'data.age' : '6', 'data.name' : 'aron' }
                ).then( function(result) {
                        if ( !result ) {
                            deferred.reject('Failed nested find');
                        }
                        if ( result.length != 1 ) {
                            deferred.reject('Find by key search');
                        }
                        if ( result[0]['data']['name'] !== 'aron') {
                            deferred.reject('Failed find by ID, found wrong result');
                        }
                    });
            })

            // find
            .then( function( ) {
                return persistence.find('myCollection',
                    { 'data.age' : '6' }
                ).then( function(result) {
                        if ( result.length != 2 ) {
                            deferred.reject('Failed find multiple results');
                        }
                    });
            })

            // find using gt operation
            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$gt' : 2 } }
                ).then( function(result) {
                        if ( result.length != 3 ) {
                            deferred.reject('Failed gt');
                        }
                    });
            })

            // find using gt operation
            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$gte' : 2 } }
                ).then( function(result) {
                        if ( result.length != 4 ) {
                            deferred.reject('Failed gt');
                        }
                    });
            })

            // find using lt operation
            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$lt' : 3 } }
                ).then( function(result) {
                        if ( result.length != 2 ) {
                            deferred.reject('Failed lt');
                        }
                    });
            })

            // find using lte operation
            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$lte' : 3 } }
                ).then( function(result) {
                        if ( result.length != 3 ) {
                            deferred.reject('Failed lt');
                        }
                    });
            })

            // find using lt and gt operation (range)
            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$gt' : 2,  '$lt' : 5 } }
                ).then( function(result) {
                        if ( result.length != 2 ) {
                            deferred.reject('Failed range (gt, lt)');
                        }
                    });
            })

            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$in' : [ 2, 3 ] } }
                ).then( function(result) {
                        if ( result.length != 2 ) {
                            deferred.reject('Failed in');
                        }
                    });
            })

            // find using cursor
            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$lt' : 3 } }, true
                ).then( function(cursor) {

                        var p = q.defer();
                        var foundCount = 0;
                        cursor.on('data', function(entry, key) {
                            if ( [1,2].indexOf( entry['data']['number']) < 0 ) {
                                deferred.reject("failed cursor");
                            } else {
                                foundCount++;
                            }
                        });

                        cursor.on('end', function() {
                            if ( foundCount != 2 ) {
                                deferred.reject('failed cursor');
                            } else {
                                p.resolve();
                            }
                        });

                        cursor.on('error', function(e) {
                            p.reject(e);
                        });
                    });
            })

            // find using iterator
            .then( function( ) {
                return persistence.find('myOtherCollection',
                    { 'data.number' : { '$lt' : 3 } }, true
                ).then( function(cursor) {
                        var foundCount = 0;
                        var deferred = q.defer();
                        var process = function(entry) {
                            var p = q.defer();
                            if ( [1,2].indexOf( entry['data']['number']) < 0 ) {
                                deferred.reject("failed cursor");
                            } else {
                                foundCount++;
                                deferred.resolve();
                            }
                            return p.promise;
                        };

                        function processItem(err, item) {
                            if(item === null) {

                                if ( foundCount != 2 ) {
                                    deferred.reject('failed cursor');
                                } else {
                                    deferred.resolve();
                                }
                                return;
                            }

                            process(item).then( function() {
                                cursor.next(processItem);
                            })
                        }

                        cursor.next(processItem);

                        return deferred.promise;
                    });
            })

            .then( function() {
                deferred.resolve();
            });
    } catch( e ) {
        deferred.reject(e);
    }

    return deferred.promise;
};

exports.testRemove = function(testUtils, persistence, collection ) {
    var deferred = q.defer();

    var obj = {
        'data' : testUtils.guid()
    };

    try {
        var key = testUtils.guid();
        collection = collection || testUtils.guid();
        persistence.save(collection, key, obj ).then( function(saved) {
            if ( !saved ) {
                deferred.reject(new Error('Did not save object'));
            }
            persistence.remove( collection, key )
                .then( function() {
                    persistence.findByID(collection, key).then( function(found) {
                        if ( found ){
                            deferred.reject(new Error("Did not expect to find deleted object"));
                        } else {
                            deferred.resolve();
                        }
                    });
                });
        });
    } catch( e ) {
        deferred.reject(e);
    }

    return deferred.promise;
};

//exports.testRemoveObject = function(testUtils, persistence ) {
//    var deferred = q.defer();
//
//    var obj = {
//        'data' : testUtils.guid(),
//        'foo': 'bar'
//    };
//
//    try {
//        var key = testUtils.guid();
//        var collection = testUtils.guid();
//        persistence.save(collection, key, obj ).then( function(saved) {
//            if ( !saved ) {
//                deferred.reject(new Error('Did not save object'));
//            }
//            persistence.remove( collection, {'foo': 'bar'} )
//                .then( function() {
//                    persistence.findByID(collection, key).then( function(found) {
//                        if ( found ){
//                            deferred.reject(new Error("Did not expect to find deleted object"));
//                        } else {
//                            deferred.resolve();
//                        }
//                    });
//                });
//        });
//    } catch( e ) {
//        deferred.reject(e);
//    }
//
//    return deferred.promise;
//};
