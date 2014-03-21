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
      var view = new View(name, db, sourceDB, mapFun, reduceFun);
      view.db.get('_local/lastSeq', function (err, lastSeqDoc) {
        if (err) {
          if (err.name !== 'not_found') {
            return cb(err);
          } else {
            view.seq = 0;
          }
        } else {
          view.seq = lastSeqDoc.seq;
        }
        cb(null, view);
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