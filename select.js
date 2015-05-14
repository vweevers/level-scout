var select   = require('json-select/select')
  , through2 = require('through2').obj

module.exports = function (selector, opts) {
  if (!selector) return through2()
  if (!Array.isArray(selector)) selector = [selector]

  var kv = opts ? opts.kv : null

  return through2(function(tuple, _, next){
    if (typeof tuple !== 'object') return next()
    if (kv == null) kv = 'key' in tuple && 'value' in tuple

    if (kv) tuple.value = select(tuple.value, selector)
    else tuple = select(tuple, selector)

    next(null, tuple)
  })
}
