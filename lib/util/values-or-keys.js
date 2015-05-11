var through2 = require('through2').obj

module.exports = function (stream, opts) {
  var values = !opts || opts.values!==false
    , keys   = !opts || opts.keys!==false

  if (values && keys) return stream

  return stream.pipe(through2(function(kv, _, next){
    next(null, values ? kv.value : kv.key)
  }))
}
