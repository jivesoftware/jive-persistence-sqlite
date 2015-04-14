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
var jive = require('jive-sdk');

q.longStackSupport = true;

/**
 * Constructor
 * @param db object
 * @constructor
 */
function SqliteClient(db) {
    this.db = db;
}

SqliteClient.prototype.query = function(sql, values) {
    var self = this;
    var p = q.defer();

    jive.logger.debug(sql, values);
    try {
        self.db.serialize(function() {
            sql = sql.replace(/\$[0-9]+/g, '?');
            var query = sql.toLowerCase();
            if ( query.indexOf('select') > -1 ) {
                // its a select
                self.db.all(sql, values, function(err, rows) {
                    if ( err ) {
                        jive.logger.error(err);
                        self.result = null;
                        p.reject(err);
                    } else {
                        // success
                        // set the rowCount value
                        self.result = {
                            rows : rows,
                            rowCount : rows.length
                        };
                    }
                    p.resolve(self);
                });
            } else {
                // its something else
                self.db.run(sql, values, function(err) {
                    if ( err ) {
                        jive.logger.error(err);
                        self.result = null;
                        p.reject(err);
                    } else {
                        // success
                        // set the rowCount value
                        self.result = {
                            rowCount : this.changes > 0 ? this.changes : 0
                        };
                        p.resolve(self);
                    }
                });
            }
        });
    } catch ( e ) {
        // no matter what ... always call done
        console.log(e.stack);
        self.released = true;
        p.reject(e);
    }

    return p.promise;
};

SqliteClient.prototype.rawClient = function() {
    return this.db;
};

SqliteClient.prototype.release = function() {
    // noop
};

SqliteClient.prototype.results = function() {
    return this.result;
};

module.exports = SqliteClient;
