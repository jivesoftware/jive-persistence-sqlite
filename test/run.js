var testUtils = require('jive-testing-framework/testUtils');
var jive = require('jive-sdk');
var jiveSqlite = require('../sqlite-dynamic');
var jiveSqliteBase = require('../sqlite-base');

var makeRunner = function() {
    return testUtils.makeRunner( {
        'eventHandlers' : {
            'onTestStart' : function(test) {
                test['ctx']['persistence'] = new jiveSqlite({});
                test['ctx']['persistenceBase'] = new jiveSqliteBase({});
            },
            'onTestEnd' : function(test) {
                test['ctx']['persistence'].destroy();
                test['ctx']['persistenceBase'].destroy();
            }
        }
    });
};

makeRunner().runTests(
    {
        'context' : {
            'testUtils' : testUtils,
            'jive' : jive,
            'jiveSqlite' : jiveSqlite
        },
        'rootSuiteName' : 'jive',
        'runMode' : 'test',
        'testcases' : process.cwd()  + '/library',
        'timeout' : 500000
    }
).then( function(allClear) {
    if ( allClear ) {
        process.exit(0);
    } else {
        process.exit(-1);
    }
});