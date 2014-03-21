'use strict';

var PouchDB = require('pouchdb');

module.exports = function (sourceDB, mapFun, reduceFun, cb) {
  sourceDB.info(function (err, info) {
    if (err) {
      return cb(err);
    }
    var name = info.db_name + '-mrview-' + PouchDB.utils.Crypto.MD5(mapFun.toString() +
      (reduceFun && reduceFun.toString()));
    var pouchOpts = {auto_compaction : true, adapter : sourceDB.adapter};
    new PouchDB(name, pouchOpts, function (err, db) {
      if (err) {
        return cb(err);
      }
      var index = new View(name, db, sourceDB, mapFun, reduceFun);
      index.db.get('_local/lastSeq', function (err, lastSeqDoc) {
        if (err) {
          if (err.name !== 'not_found') {
            return cb(err);
          } else {
            index.seq = 0;
          }
        } else {
          index.seq = lastSeqDoc.seq;
        }
        cb(null, index);
      });
    });
  });
};

function View(name, db, sourceDB, mapFun, reduceFun) {
  this.db = db;
  this.name = name;
  this.sourceDB = sourceDB;
  this.adapter = sourceDB.adapter;
  this.mapFun = mapFun;
  this.reduceFun = reduceFun;
}