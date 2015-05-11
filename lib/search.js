var through2 = require('through2')
  , searchStream = require('./streams/search')

module.exports = search

search.install = function(db) {
  db.search = search.bind(null, db)
}

function search(db, query, opts, cb) {
  if (typeof opts == 'function') cb = opts, opts = null
  var stream = searchStream(db, query, opts)
  return cb ? concat(stream, cb) : stream
}

function concat(stream, cb) {
  var acc = []

  return stream.once('error', done)
    .pipe(through2.obj(function(o, _, next){
      acc.push(o)
      next(null, o)
    })).on('finish', done).once('error', done)

  function done(err) {
    cb && cb(err, acc, stream.plan)
    cb = null
  }
}
