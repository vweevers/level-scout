var after             = require('after')
  , garbageCollection = require('./lib/util/gc')
  , tree              = require('./lib/util/property-tree')
  , Index             = require('./lib/index')

module.exports = function createIndex(db, props, opts) {
  if (!db.indexes) initDatabase(db, { gc: opts && opts.gc })

  var name = (props = [].concat(props)).join()

  if (!db.indexes[name]) {
    db.indexes[name] = new Index(db, name, props, opts)
    tree.add(db.propertyTree, db.indexes[name])
  }

  return db.indexes[name]
}

module.exports.install = function(db) {
  db.index = module.exports.bind(null, db)
}

function initDatabase(db, opts) {
  // TODO: wrap db, set options there
  db.indexes = Object.create(null)
  db.propertyTree = tree.create()

  var gc = db.gc = garbageCollection(db, opts && opts.gc)

  db.pre(function(op, add, ops){
    if (op.type != 'del') {
      var before = ops.length

      for(var k in db.indexes)
        db.indexes[k].changed(op.key, op.value, add)

      // Cancel scheduled gc ops, if any
      var added = ops.length - before
      gc.cancel(op.key, added > 0 && ops.slice(-added))
    }

    // A preAll() hook would be nice
    gc.clear().forEach(add)
  })

  db.post(function(op){
    if (op.type != 'del') return

    var add  = gc.start(op.key)
      , done = after(Object.keys(db.indexes).length, function(){
        gc.end(op.key)
        db.emit('gc', op.key)
      })

    for(var k in db.indexes) db.indexes[k].deleted(op.key, add, done)
  })
}
