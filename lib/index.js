var through2          = require('through2').obj
  , emitter           = require('events').EventEmitter
  , viewStream        = require('./streams/view')
  , normalizeRange    = require('./util/range').normalize
  , defaultMap        = require('./value-mapper/default')
  , bytespace         = require('bytespace')
  , bytewise          = bytespace.bytewise

module.exports = Index

function Index(parent, name, props, opts) {
  // TODO: inverse value encoding should just be utf8 or binary
  this.db = bytespace(parent, 'index_' + name,   { valueEncoding: 'json', keyEncoding: bytewise })
  this.inverse = bytespace(this.db, 'inverse',   { valueEncoding: 'json', keyEncoding: bytewise })
  this.metadata = bytespace(this.db, 'metadata', { valueEncoding: 'json', keyEncoding: 'utf8'   })
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

  this.fetchStatistics()
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

Index.prototype.update = function(entityKey, entity, add) {
  var indexKey = this.map(entityKey, entity)
  if (indexKey == null) return

  var inverse = [entityKey].concat(indexKey)
  indexKey.push(entityKey)

  // todo: shouldn't bytespace infer the encoding from the prefix?
  add({prefix: this.db,      key: indexKey, value: entityKey, keyEncoding: bytewise })
  add({prefix: this.inverse, key: inverse,  value: null     , keyEncoding: bytewise })
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
      {type: 'del', prefix: this.inverse, key: inverseKey, keyEncoding: bytewise},
      {type: 'del', prefix: this.db, key: indexKey, keyEncoding: bytewise}
    ]

    if (add(ops)===false) stream.destroy()
  }.bind(this)).on('end', done)
}

Index.prototype.fetchStatistics = function() {
  this.cardinality = 0  // Number of distinct values
  this.length = 0       // Number of total values

  this.metadata.get('statistics', function(err, stats){
    if (stats) {
      this.length = stats.length
      this.cardinality = stats.cardinality
    }

    this.emit('ready')
  }.bind(this))
}

Index.prototype.saveStatistics = function(stats, done) {
  this.cardinality = stats.cardinality
  this.length = stats.length
  this.metadata.put('statistics', stats, done)
}

// Selectivity = cardinality / number of rows
Index.prototype.selectivity = function() {
  return this.length && this.cardinality
    ? this.cardinality / this.length : 0;
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
    index.update(kv.key, kv.value, add)
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
