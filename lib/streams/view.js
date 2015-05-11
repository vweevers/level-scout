var through2 = require('through2').obj

module.exports = function(db, opts) {
  var values = opts && (opts.values && !opts.keys)
    , keys = opts && (opts.keys && !opts.values)

  return through2(function(key, _, next){
    db.get(key, function(err, entity){
      if (err) return next()

      if (values) return next(null, entity)
      if (keys) return next(null, key)

      next(null, {key: key, value: entity})
    })
  })
}
