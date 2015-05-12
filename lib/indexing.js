var through2 = require('through2').obj
  , normalizeRange = require('./util/range').normalize
  , defaultMap = require('./value-mapper/default')
  , emitter = require('events').EventEmitter
  , viewStream = require('./streams/view')
  , debug = require('debug')('level-scout')
  , objectID = require('sigmund')
  , HyperLogLog = require('hyperloglog')
  , garbageCollection = require('./util/gc')
  , after = require('after')
  , tree = require('./util/property-tree')

function createIndex(db, props, opts) {
  if (!db.indexes) initDatabase(db, { gc: opts && opts.gc })

  var name = (props = [].concat(props)).join()

  if (!db.indexes[name]) {
    db.indexes[name] = new Index(db, name, props, opts)
    tree.add(db.propertyTree, db.indexes[name])
  }

  return db.indexes[name]
}

createIndex.install = function(db) {
  db.index = createIndex.bind(null, db)
  return db
}

module.exports = createIndex

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

function Index(parent, name, props, opts) {
  this.db = parent.sublevel('index_' + name)
  this.inverse = this.db.sublevel('inverse')
  this.metadata = this.db.sublevel('metadata')
  this.parent = parent
  
  this.name = name
  this.properties = props

  if (typeof opts == 'function') this._map = opts, opts = null
  else this._map = (opts && opts.map) || defaultMap

  // Decorating
  this.get = this.db.get.bind(this.db)
  this.normalizeRange = normalizeRange.bind(null, props.length + 1)
  this.map = this.map.bind(this)
  this.map.properties = props // temporary (TODO: maybe decouple mapping from indexing)

  this.initializeStats()
}

require('inherits')(Index, emitter)

Index.prototype.map = function(entityKey, entity, subset) {
  var indexKey = this._map(entityKey, entity, subset || this.properties)

  // In case the map function does not honour the subset argument
  if (indexKey != null && subset != null && indexKey.length !== subset.length) {
    indexKey = subset.map(function(property){
      return indexKey[this.indexOf(property)]
    }, this.properties)
  }

  return indexKey
}

Index.prototype.changed = function(entityKey, entity, add) {
  var indexKey = this.map(entityKey, entity)
  if (indexKey == null) return

  var inverse = [entityKey].concat(indexKey)
    , uniqID  = objectID(indexKey)

  indexKey.push(entityKey)

  add({prefix: this.db,      key: indexKey, value: entityKey })
  add({prefix: this.inverse, key: inverse,  value: null      })

  this.updateStats(uniqID, add)
}

Index.prototype.deleted = function(entityKey, add, done) {
  var range = { lt: [entityKey, undefined], gt: [entityKey, null] }

  // Note: if the process crashes or the batch fails, delete 
  // won't happen. Keys will eventually be removed though, 
  // by post-read GC (on the todo list).
  var stream = this.createInverseKeyStream(range).on('data', function(inverseKey){
    var indexKey = inverseKey.slice(1)
    indexKey.push(entityKey)

    var ops = [
      {type: 'del', prefix: this.inverse, key: inverseKey},
      {type: 'del', prefix: this.db, key: indexKey}
    ]

    if (add(ops)===false) stream.destroy()
  }.bind(this)).on('end', done)
}

Index.prototype.initializeStats = function() {
  this.hll = HyperLogLog(8) // Standard error is 6.5%
  this.length = 0

  this.metadata.get('hll', function(err, prev){
    if (!prev) return

    this.length+= prev.length
    this.hll.merge({n: prev.n, buckets: new Buffer(prev.buckets, 'base64')})
  }.bind(this))
}

// TODO: only save metadata every n entities
// TODO: update in post hook, save with a trigger?
// TODO: include deletes
Index.prototype.updateStats = function(id, add) {
  this.hll.add(HyperLogLog.hash(id))
  this.length++;
  
  // { n: bit size, buckets: Buffer }
  var hll = this.hll.output()

  hll.buckets = hll.buckets.toString('base64')
  hll.length = this.length

  add({prefix: this.metadata, key: 'hll', value: hll})
}

// selectivity = cardinality / number of rows
Index.prototype.selectivity = function() {
  return this.length ? this.hll.count() / this.length : null;
}

Index.prototype.createViewStream = function(opts) {
  return this.createValueStream(opts)
    .pipe(viewStream(this.parent, opts))
}

Index.prototype.createKeyStream = function(opts) {
  if (opts) opts = this.normalizeRange(opts)
  return this.db.createKeyStream(opts)
}

Index.prototype.createValueStream = function(opts) {
  if (opts) opts = this.normalizeRange(opts)
  return this.db.createValueStream(opts)
}

Index.prototype.createInverseKeyStream = function(opts) {
  if (opts) opts = this.normalizeRange(opts)
  return this.inverse.createKeyStream(opts)
}

Index.prototype.rebuild = function(cb) {
  if (cb) this.once('build', cb)
  if (this.building) return

  this.building = true
  var index = this

  function done(err){
    index.building = false
    index.emit('build', err)
  }

  // TODO: empty before?
  // TODO: use batched writestream
  // TODO: emit warning if pre hook is called during rebuild
  this.parent.createReadStream().pipe(through2(function(kv, _, next){
    var ops = [], add = ops.push.bind(ops)
    index.changed(kv.key, kv.value, add)
    return ops.length ? index.db.batch(ops, next) : next()
  })).on('finish', done).on('error', done)
}

Index.prototype.sweep = function() {
  // TODO. Stream inverse keys. Check if entity key exists, map, then remove old
  // TODO: Schedule sweep after a read if overhead is above a certain threshold
}

Index.prototype.inspect = function() {
  return this.name
}
