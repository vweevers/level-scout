var through2          = require('through2').obj
  , emitter           = require('events').EventEmitter
  , objectID          = require('sigmund')
  , HyperLogLog       = require('hyperloglog')
  , viewStream        = require('./streams/view')
  , normalizeRange    = require('./util/range').normalize
  , defaultMap        = require('./value-mapper/default')

module.exports = Index

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
  this.map.properties = props // Temporary (TODO: maybe decouple mapping from indexing)

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

// Selectivity = cardinality / number of rows
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
