/*
 * database.js: Configuration for CouchDB and cradle. 
 *
 * Mike Bogochow (mbogochow)
 */
 
var cradle = require('cradle');

// Setup a cradle interface for a CouchDB server.
// If database does not exist then create it.
var setup = exports.setup = function (options, callback) {
  if (typeof options == 'function') {
    callback = options;
    options = {};
  }
  // Set connection configuration
  cradle.setup({
    host: options.host || '127.0.0.1',
    port: options.port || 5984,
    cache: options.cache,
    options: options.options
  });
  
  // Connect to cradle
  var conn = new (cradle.Connection)({ auth: options.auth }),
      db   = conn.database(options.database || 'default');
      
  db.exists(function (err, exists) {
    if (err) return callback(err, null);
    if (!exists) db.create(); 
    
    callback(null, db);
  });
  
  return conn;
}

// Return given database from given cradle connection.  Used for updating 
// database object to apply remote changes.
var update = exports.update = function (conn, database, callback) {
  var db = conn.database(database);
  
  db.exists(function (err, exists) {
    if (err) return callback(err, null);
    if (!exists) return callback('database does not exist', null);
    
    callback(null, db);
  });
}

// Add an entry into the given database
// redundant just use db.save
var addEntry = exports.addEntry = function (db, data, callback) {
  db.save(data, function (err) {
    if (err) return callback(err);
    callback(null, db);    
  });
};

// Get a specific entry by id from the given database
var getEntry = exports.getEntry = function (db, id, options, callback) {
  if (typeof options == 'function') {
    callback = options;
    options = {};
  }
  
  db.get(id, function (err, doc) {
    if (err) return callback(err);
    
    if (options.escape = true) unescapeDocument(doc);
    
    callback(null, doc);
  });
};

var escapeDocument = exports.escapeDocument =  function (obj) {
  for (key in obj) {
    if (typeof obj[key] == 'object') escapeDocument(obj[key]);
    else {
      obj[key] = escape(obj[key]);
    }
  }
  
  return obj;
}

var unescapeDocument = exports.unescapeDocument = function (obj) {
  for (key in obj) {
    if (typeof obj[key] == 'object') unescapeDocument(obj[key]);
    else obj[key] = unescape(obj[key]);
  }
  
  return obj;
}

// List all entries in the given database
// Currently relies on a built-in view
var listAll = exports.listAll = function (db, callback) {
  db.view('user/byUsername', callback);
};

var list = exports.list = function (db, view, callback) {
  db.view(view, callback);
};

// Perform given function on each document in given database
var processDocuments = exports.processDocuments = function (db, fn) {
  var viewDoc = '_design/mbDB';
  db.save(viewDoc, {
    views: {
      all: {
        map: 'function (doc) { emit(doc, doc) }'
      }
    }
  });
  
  db.view('mbDB/all', function (err, arr) {
  console.dir(arr);
    if (err) return console.dir(err);
    for (var i = 0; i < arr.length; i++) {
      fn(arr[i].value);
      updateEntry(db, arr[i].value._id, arr[i].value, function (err) {
        if (err) return console.dir(err);
        
        db.remove(viewDoc, function(err2) {
          if (err2) return console.dir(err);
        });
      });
    }
  });
}

// Search the given database for tags of the given key
// Currently does not work
var search = exports.search = function (db, key, callback) {
  db.save('_design/custom', {
    views: {
      query: {
        map: 'function (doc) {' +
          'if (doc.attributes) {' +
            'if (doc.attributes.tags.indexOf(key) > -1) {' +
              'emit(doc.title, doc);' +
            '}' +
          '}'
      }
    }
  }, callback);
  //db.view('custom/query', callback);
};

// Updates an existing entry in the given database.  Note that the document 
// will be identical to entry.
// redundant just use db.merge
var mergeEntry = exports.mergeEntry = function (db, id, entry, callback) {
  db.merge(id, entry, function (err, res) {
    if (err) {
      return callback(err);
    }
    
    callback(null, true);
  });
};

var updateEntry = exports.updateEntry = function (db, id, entry, callback) {
  db.get(id, function (err, doc) {
    if (err) {
      if (err.error == 'not_found') {
        db.save(entry, function (err2, res) {
          if (err2) {
            return callback(err2, false);
          }
          return callback(null, true);
        });
      } else {
        return callback(err, false);
      }
      
      return;
    }
    db.save(id, doc._rev, entry, function (err3, res) {
      if (err3) {
        return callback(err3, false);
      }
      
      callback(null, true);
    });
  });
};

// Remove an entry in the given database.
// Not tested
var removeEntry = exports.removeEntry = function (db, id, callback) {
  var self = this;
  getEntry(db, id, function (err, doc) {
    if (err) {
      return callback(err);
    }
    
    db.remove(id, doc._rev, function (err, res) {
      if (err) {
        return callback(err);
      }

      callback(null, true);
    });
  });
};
