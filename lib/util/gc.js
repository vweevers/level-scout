var sigmund = require('sigmund')

// Optimistic batch queue for garbage collection
// ops. Operations can be canceled by entity key.

module.exports = function(db, opts) {
  opts || (opts = {})

  var size      = opts.size || 50
    , queue     = Object.create(null)
    , numQueued = 0
    , active    = Object.create(null)
    , numActive = 0
    , writing   = null
    , rewrites  = []
    , gc        = {}
    , timeout
    , delay     = opts.delay || 1000 * 30

  gc.start = function(entityKey) {
    var h = sigmund(entityKey)

    active[h] = true
    numActive++

    return function(ops) {
      if (active[h]) add(entityKey, ops, h)
      else return false
    }
  }

  function add(entityKey, ops, _hash) {
    var h = _hash || sigmund(entityKey)

    queue[h] = (queue[h] || []).concat(ops)
    numQueued+= ops.length

    if (numQueued >= size) gc.flush()
    else {
      clearTimeout(timeout)
      timeout = setTimeout(gc.flush, delay)
      timeout.unref && timeout.unref()
    }
  }

  gc.clear = function() {
    var ops = []
    clearTimeout(timeout)

    if (numQueued) {
      for(var k in queue) if (queue[k]) ops = ops.concat(queue[k])
      queue = Object.create(null)
      numQueued = 0
    }

    return ops
  }

  // Don't accept any more additions
  gc.end = function(entityKey, _hash) {
    var h = _hash || sigmund(entityKey)

    if (active[h]) {
      active[h] = false
      if (--numActive === 0) active = Object.create(null)
    }
  }

  gc.cancel = function(entityKey, ops) {
    var h = sigmund(entityKey)

    this.end(entityKey, h)

    if (queue[h] != null) {
      numQueued-= queue[h].length
      queue[h] = null
    } else if (ops && writing && writing[h]) {
      // Schedule a "rewrite" for the new ops
      rewrites.push(add.bind(null, entityKey, ops, h))
    }
  }

  gc.flush = function(cb) {
    if (!numQueued || writing) return cb && setImmediate(cb);

    clearTimeout(timeout)
    writing = queue

    db.batch(gc.clear(), function(){
      writing = null

      var a = rewrites
      rewrites = []

      cb && cb()

      if (a.length) a.forEach(function(cb){ cb() })
      else if (numQueued >= size) gc.flush()
    })
  }

  return gc
}
