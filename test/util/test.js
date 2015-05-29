var index     = require('../../index')
  , search    = require('../../search')
  , bytespace = require('bytespace')
  , xtend     = require('xtend')
  , tape      = require('tape')
  , levelup   = require('levelup')
  , memdown   = require('memdown')
  , rimraf    = require('rimraf')
  , mkdirp    = require('mkdirp')
  , tmpdir    = require('osenv').tmpdir()
  , path      = require('path')

require('./tape-debug')

var defs      = { valueEncoding: 'json' }
  , num       = 0
  , down      = disk('level-scout')
  , mem       = levelup('mem', { db: memdown })

module.exports = test.bind(null, tape)
module.exports.skip = test.bind(null, tape.skip)
module.exports.only = test.bind(null, tape.only)

function test(method, name, opts, body) {
  if (typeof opts == 'function') body = opts, opts = {}

  // Test without database
  if (opts === false) return method(name, body)

  method(name, function(t){
    t.test('[memdown] '+name, function(t){
      body(t, create(mem, opts))
    })

    down && t.test('[leveldown] '+name, function(t){
      body(t, create(down, opts))
    })
  })  
}

function create(db, opts) {
  var sdb = bytespace(db, String(++num), xtend(defs, opts))

  if (opts.index)  index.install(sdb)
  if (opts.search) search.install(sdb)

  return sdb
}

function disk (name, opts, cb) {
  opts || (opts = {})

  try {
    var leveldown = require('leveldown')
  } catch (err) {
    console.error('Could not load leveldown, skipping tests')
    return false
  }

  mkdirp.sync(tmpdir)
  var dir = path.join(tmpdir, name)

  if (opts.clean !== false) rimraf.sync(dir)
  opts.db = leveldown

  return levelup(dir, opts, cb)
}
