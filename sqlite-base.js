/*
 * Copyright 2013 Jive Software
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


var q = require('q');
    q.longStackSupport = true;
var jive = require('jive-sdk');
var SQLiteClient = require('./sqlite-client');

module.exports = function(serviceConfig) {
    var sqlite3 = require('sqlite3').verbose();
    var db = new sqlite3.Database(':memory:');

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Init

    jive.logger.info("SQLite ready.");

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Private

    function requestClient(clientID, deferred) {
        var sqliteClient = new SQLiteClient(db);
        deferred.resolve(sqliteClient);
    }

    function getClient() {
        var deferred = q.defer();

        var clientID = jive.util.guid();
        requestClient(clientID, deferred);

        return deferred.promise;
    }

    function query(sql) {
       return getClient().then( function(client) {
           return client.query(sql);
       });
    }

    function destroy() {
        var p = q.defer();
        return p.promise;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Public API

    return {
        query : query,
        destroy: destroy,
        getClient : getClient
    };
};
