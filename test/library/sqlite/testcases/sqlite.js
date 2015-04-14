var assert = require('assert');
var test = require('../basePersistenceTest');

describe('jive', function () {

    describe ('#persistence.sqlite', function () {

        it('test', function (done) {
            var jive = this['jive'];
            var testUtils = this['testUtils'];
            var persistence = this['persistence'];

            var toSync = {
                'tbl' : {
                    data: { type: "text", required: false, expandable: true }
                },
                'myCollection' : {
                    key:       { type: "text", required: false },
                    data_name: { type: "text", required: false },
                    data_age:  { type: "text", required: false }
                },
                'myOtherCollection' : {
                    data_number:    { type: "text", required: false }
                }
            };

            persistence.sync( toSync, true)
            .then( function() {
                return persistence.save('tbl', '1', {
                    'data' : {
                        'name' : 'aron'
                    }
                });
            })
            .then( function() {
                return test.testSave(testUtils, persistence, 'tbl');
            })
            .then( function() {
                return test.testFind(testUtils, persistence);
            })
            .then( function() {
                return test.testRemove(testUtils, persistence, 'tbl');
            })
//            .then( function() {
//                return test.testRemoveObject(testUtils, persistence, 'tbl');
//            })
            .catch( function(e) {
                assert.fail(e);
            })
            .finally( function() {
                done();
            });

        });

    });
});

